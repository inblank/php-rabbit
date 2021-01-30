<?php

namespace inblank\rabbit;

use AMQPConnection;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

/**
 * Работа с брокером сообщений
 * На базе: {@link http://ajaxblog.ru/php/rabbitmq-tutorial/}
 */
class Connection
{
    /**
     * Подключение к серверу брокера сообщений
     * @var AMQPConnection|null
     */
    private ?\AMQPConnection $connection = null;
    /**
     * Канал подключения
     * @var \AMQPChannel|null
     */
    private ?\AMQPChannel $channel = null;
    /**
     * Конфигурация подключения, обменников и очередей
     *
     *  - **connection** - данные подключения
     *      - **host** - хост подключения к серверу
     *      - **port** - порт подключения
     *      - **vhost** - имя виртуального хоста
     *      - **login** - логин пользователя
     *      - **password** - пароль пользователя
     *  - **exchanges** - описание обменников. Ключ имя обменника. Значения параметры обменника.
     *      - **type** - тип обменника
     *      - **flags** - флаги
     *      - **bind** - список привязок очередей к обменнику. Формат: 'queueName' или  'queueName' => 'routingKey'
     *  - **queues** - описание очередей. Ключ имя очереди. Значения параметры обменника.
     *      - **flags** - флаги
     *
     * @var array
     */
    public array $config = [];
    /**
     * Имя log файла
     * По умолчанию /var/log/phprabbit/log.log
     * @var string
     */
    protected string $logFile;
    /**
     * Логгер
     * @var Logger
     */
    protected Logger $logger;
    /**
     * Величины задержек в миллисекундах перед попытками подключения при потере соединения
     * Количество задержке задает количество попыток.
     * @var int[]
     */
    public array $retry = [100, 1000, 2000];

    /**
     * Конструктор
     * @param array $config конфигурация подключения. Формат в {@see Connection::$config}
     * @param string|null $logFile имя лог файла. {@see Connection::$logFile}
     */
    public function __construct(array $config, ?string $logFile = null)
    {
        $this->config = $config;
        $this->logFile = $logFile ?? "/var/log/phprabbit/log.log";
    }

    /**
     * Получение подключения к серверу
     * @return AMQPConnection
     * @throws \AMQPConnectionException
     */
    protected function getConnection(): \AMQPConnection
    {
        if ($this->connection === null) {
            try {
                $this->connection = new AMQPConnection($this->config['connection'] ?? null);
                $this->connection->connect();
                $try = 0;
                $attempts = count($this->retry);
                while (!($connected = $this->connection->isConnected()) && $try < $attempts) {
                    // пытаемся переподключиться в случае потери соединения
                    usleep($this->retry[$try++]);
                    $this->connection->reconnect();
                }
                if (!$connected) {
                    // не смогли подключиться
                    throw new \AMQPConnectionException();
                }
            } catch (\Throwable $e) {
                $host = $this->config['connection']['host'] ?? '127.0.0.1';
                $message = $e->getMessage();
                $this->exception("Can't connect to rabbit server: {$host}" . (!empty($message) ? " ({$message})" : ''));
            }
        }
        return $this->connection;
    }

    /**
     * Получение канала работы с сервером
     * @return \AMQPChannel
     * @throws \AMQPConnectionException
     */
    public function getChannel(): \AMQPChannel
    {
        if ($this->channel === null) {
            $this->channel = new \AMQPChannel($this->getConnection());
        }
        return $this->channel;
    }

    /**
     * Получение обменника
     * @param string $name имя обменника который получить. Должен быть описан в конфигурации {@see Connection::$config}
     * @return Exchange
     */
    public function getExchange(string $name): Exchange
    {
        return Exchange::getInstance($this, $name);
    }

    /**
     * Получение очереди
     * @param string $name имя очереди которую получить. Должна быть описана в конфигурации {@see Connection::$config}
     * @return Queue
     */
    public function getQueue(string $name): Queue
    {
        return Queue::getInstance($this, $name);
    }

    /**
     * Повторное подключение
     * @return bool если удалось переподключиться true, иначе false
     */
    public function reconnect(): bool
    {
        if ($this->connection->isConnected()) {
            $this->connection->disconnect();
        }
        Exchange::reset();
        Queue::reset();
        $this->connection = $this->channel = null;
        try {
            $this->getConnection();
            return true;
        } catch (\AMQPConnectionException $e) {
            // не смогли подключиться
            return false;
        }
    }

    /**
     * Выброс исключения с записью ошибки
     * @param string $message сообщение об ошибке
     * @throws \AMQPConnectionException
     */
    public function exception(string $message): void
    {
        $this->getLogger()->error($message);
        throw new \AMQPConnectionException($message);
    }

    /**
     * Получение логгера
     * @return Logger
     */
    public function getLogger(): Logger
    {
        if (!isset($this->logger)) {
            $dir = dirname($this->logFile);
            if (!((is_dir($dir) || mkdir($dir, 0777, true)) && is_writable($dir))) {
                throw new \RuntimeException("Directory `{$dir}` not writable");
            }
            $stream = (new StreamHandler($this->logFile))->setFormatter(
                new LineFormatter(
                    "[%datetime%]\t%channel%.%level_name%\t%message%\t%context%\t%extra%\n",
                    'Y-m-d H:i:s'
                )
            );
            $this->logger = (new Logger('rabbit'))->pushHandler($stream);
        }
        return $this->logger;
    }

    /**
     * Деструктор
     */
    public function __destruct()
    {
        if (isset($this->connection) && $this->connection->isConnected()) {
            Exchange::reset();
            Queue::reset();
            $this->connection->disconnect();
        }
    }
}