<?php

namespace inblank\rabbit;

use AMQPConnection;
use Exception;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

/**
 * Работа с rabbit
 * На базе: http://ajaxblog.ru/php/rabbitmq-tutorial/
 */
class rabbit
{
    /**
     * Подключение к серверу брокера сообщений
     * @var AMQPConnection
     */
    private \AMQPConnection $connection;
    /**
     * Канал подклбчения
     * @var \AMQPChannel
     */
    private \AMQPChannel $channel;
    /**
     * Инициализированные обменники
     * @var array
     */
    private array $exchanges = [];
    /**
     * Инициализированные очереди
     * @var array
     */
    private array $queues = [];
    /**
     * Конфигурация
     * @var array
     */
    protected array $config = [];
    /**
     * Имя log файла
     * По умолчанию /var/log/phprabbit/log.log
     * @var string|null
     */
    protected ?string $logFile;
    /**
     * Логгер
     * @var Logger
     */
    protected Logger $logger;
    /**
     * Величины задержек в миллисекундах перед попытками подключения в случае потери соединения
     * Количество задержке задает количество попыток.
     * @var int[]
     */
    public array $retry = [100, 1000, 2000];
    /**
     * Имя активного обменника
     * @var string|null
     */
    protected ?string $activeExchange = null;
    /**
     * Имя активной очереди
     * @var string|null
     */
    protected ?string $activeQueue = null;

    /**
     * Инициализация
     * @param array $config конфигурация
     * @param string|null $logFile имя лог файла. Если не задано будет установлено в /var/log/phprabbit/log.log
     * @throws Exception
     */
    public function __construct(array $config, ?string $logFile = null)
    {
        $this->config = $config;
        $this->logFile = $logFile;
        $this->init();
    }

    /**
     * Получение подключения к серверу
     * @return AMQPConnection
     * @throws \AMQPConnectionException
     */
    protected function getConnection(): \AMQPConnection
    {
        if (!isset($this->connection) || !$this->connection->isConnected()) {
            if (!isset($this->connection)) {
                // еще не подключались
                $this->connection = new AMQPConnection($this->config['connection'] ?? null);
                $this->connection->connect();
            }
            $try = 0;
            $attempts = count($this->retry);
            while (!($connected = $this->connection->isConnected()) && $try++ < $attempts) {
                // пытаемся переподключиться в случае потери соединения
                $this->connection->reconnect();
                usleep($this->retry[$try - 1]);
            }
            if (!$connected) {
                $this->exception(
                    "Can't connect to rabbit server: " . ($this->config['connection']['host'] ?? 'localhost'),
                    'connection'
                );
            }
        }
        return $this->connection;
    }

    /**
     * Получение канал подключения к серверу
     * @return \AMQPChannel
     * @throws \AMQPConnectionException
     */
    protected function getChannel(): \AMQPChannel
    {
        if (!isset($this->channel)) {
            $this->channel = new \AMQPChannel($this->getConnection());
        }
        if (!$this->connection->isConnected()) {
            // потеряно соединение, пытаемся получить снова
            $this->getConnection();
        }
        return $this->channel;
    }

    /**
     * Получение обменника
     * @param string $name имя обменника который получить. Должен быть описан в конфигурации
     * @return \AMQPExchange
     * @throws \AMQPExchangeException
     * @throws \AMQPConnectionException
     */
    protected function getExchange(string $name): \AMQPExchange
    {
        if (!isset($this->exchanges[$name])) {
            // создаем новый обменник
            if (empty($this->config['exchanges'][$name])) {
                $this->exception("Exchange `{$name}` not defined", 'exchange');
            }
            $config = $this->config['exchanges'][$name];
            // создаем и настраиваем обменник
            $exchange = new \AMQPExchange($this->getChannel());
            $exchange->setName($name);
            if (!empty($config['type'])) {
                $exchange->setType($config['type']);
            }
            if (!empty($config['flags'])) {
                $exchange->setFlags($config['flags']);
            }
            // объявляем обменник
            try {
                if (!$exchange->declareExchange()) {
                    // не удалось объявить обменник
                    throw new \AMQPExchangeException();
                }
            } catch (\Throwable $e) {
                // что-то пошло не так
                $message = $e->getMessage();
                $this->exception(empty($message) ? "Error declare exchange `{$name}`" : $message, 'exchange');
            }
            // создаем и биндим все нужные очереди
            foreach ($this->config['exchanges'][$name]['bind'] ?? [] as $queueName => $routingKey) {
                if (is_int($queueName)) {
                    // задано без ключа роутинга
                    $queueName = $routingKey;
                    $routingKey = null;
                }
                $queue = $this->getQueue($queueName);
                $queue->bind($name, $routingKey);
            }
            $this->exchanges[$name] = $exchange;
        }
        if (!$this->connection->isConnected()) {
            // потеряно соединение, пытаемся получить снова
            $this->getConnection();
        }
        return $this->exchanges[$name];
    }

    /**
     * Установка активного обменника
     * @param string $name имя обменника который сделать активным
     * @return self
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function setExchange(string $name): self
    {
        $this->getExchange($name);
        $this->activeExchange = $name;
        return $this;
    }

    /**
     * Получение очереди
     * @param $name
     * @return \AMQPQueue
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function getQueue($name): \AMQPQueue
    {
        if (!isset($this->queues[$name])) {
            if (empty($this->config['queues'][$name])) {
                $this->exception("Queue `{$name}` not defined", 'queue');
            }
            // создаем и настраиваем очередь
            $config = $this->config['queues'][$name];
            $queue = new \AMQPQueue($this->getChannel());
            $queue->setName($name);
            if (!empty($config['flags'])) {
                $queue->setFlags($config['flags']);
            }
            // объявляем очередь
            try {
                $queue->declareQueue();
            } catch (\Throwable $e) {
                // что-то пошло не так
                $message = $e->getMessage();
                $this->exception(empty($message) ? "Error declare queue `{$name}`" : $message, 'queue');
            }
            $this->queues[$name] = $queue;
        }
        if (!$this->connection->isConnected()) {
            // потеряно соединение, пытаемся получить снова
            $this->getConnection();
        }
        return $this->queues[$name];
    }

    /**
     * Установка активной очереди
     * @param string $name имя очереди которую сделать активной
     * @return self
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function setQueue(string $name): self
    {
        $this->getQueue($name);
        $this->activeQueue = $name;
        return $this;
    }

    /**
     * Публикация сообщения в активный обменник
     * @param string $message сообщение для публикации
     * @param string|null $key ключ роутинга
     * @return bool
     * @throws \AMQPChannelException
     * @throws \AMQPExchangeException
     * @throws \AMQPQueueException
     */
    public function publish(string $message, ?string $key = null): bool
    {
        if (empty($this->activeExchange)) {
            // не выбран активный обменник
            return false;
        }
        /** @var \AMQPExchange $exchange */
        if (null === ($exchange = $this->exchanges[$this->activeExchange] ?? null)) {
            $this->exception("Undeclared exchange `{$this->activeExchange}`", 'exchange');
        }
        $try = 0;
        $attemptCount = count($this->retry);
        $published = false;
        do {
            try {
                $published = $exchange->publish($message, $key);
            } catch (\AMQPException $e) {
                // проблема с подключением
                $this->connection->reconnect();
            }
            if (!$published) {
                // ждем некоторое время перед повторной попыткой
                usleep($this->retry[$try]);
            }
            $try++;
        } while (!$published && $try < $attemptCount);
        return $published;
    }

    /**
     * Инициализация
     */
    protected function init(): void
    {
        // инициализируем логгер
        $this->logFile = $this->logFile ?? "/var/log/phprabbit/log.log";
        $dir = dirname($this->logFile);
        if (!((is_dir($dir) || mkdir($dir, 0777, true)) && is_writable($dir))) {
            throw new \RuntimeException("Directory `{$dir}` not writable");
        }
        $this->logger = new Logger('rabbit');
        $stream = new StreamHandler($this->logFile);
        $formatter = new LineFormatter(
            "[%datetime%]\t%channel%.%level_name%\t%message%\t%context%\t%extra%\n",
            'Y-m-d H:i:s'
        );
    }

    /**
     * Выброс исключения с записью ошибки
     * @param string $message сообщение об ошибке
     * @param string $type тип исключения: connection, exchange, channel, queue
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     * @throws \AMQPQueueException
     */
    protected function exception(string $message, string $type): void
    {
        $this->logger->error($message);
        switch ($type) {
            case 'connection':
                throw new \AMQPConnectionException($message);
            case 'exchange':
                throw new \AMQPExchangeException($message);
            case 'channel':
                throw new \AMQPChannelException($message);
            case 'queue':
                throw new \AMQPQueueException($message);
            default:
                throw new \RuntimeException($message);
        }
    }

    /**
     * Деструктор
     */
    public function __destruct()
    {
        if (isset($this->connection) && $this->connection->isConnected()) {
            $this->connection->disconnect();
        }
    }
}