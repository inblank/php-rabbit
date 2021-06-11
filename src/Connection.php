<?php

namespace inblank\rabbit;

use Exception;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use RuntimeException;
use Throwable;

/**
 * Работа с брокером сообщений
 * На базе: {@link http://ajaxblog.ru/php/rabbitmq-tutorial/}
 */
class Connection
{
    /**
     * Конфигурация подключения, обменников и очередей
     *
     *  - **connection** - данные подключения
     *      - **host** - хост подключения к серверу
     *      - **port** - порт подключения
     *      - **vhost** - имя виртуального хоста
     *      - **user** - имя пользователя
     *      - **password** - пароль пользователя
     *  - **exchanges** - описание обменников. Ключ имя обменника. Значения параметры обменника.
     *      - **type** - тип обменника
     *      - **flags** - флаги
     *      - **bind** - список привязок очередей к обменнику. Формат: 'queueName' или  'queueName' => 'routingKey'
     *  - **queues** - имена очередей
     *  - **logfile** - имя лог-файла. По-умолчанию: /var/log/phprabbit/log.log
     *  - **retry** - Величины задержек в миллисекундах перед попытками подключения при потере соединения
     *          Количество задержке задает количество попыток. По умолчанию: [10000, 100000, 250000]
     *
     * @var array
     */
    public array $config = [];

    /** @var AMQPStreamConnection Подключение к серверу брокера сообщений */
    private AMQPStreamConnection $connection;

    /** @var AMQPChannel Канал подключения */
    private AMQPChannel $channel;

    /** @var Logger Логгер */
    protected Logger $logger;

    /**
     * Конструктор
     * @param array $config конфигурация подключения. Формат в {@see Connection::$config}
     */
    public function __construct(array $config)
    {
        // если задан один сервер подключения, приводим к формату
        isset($config['connection']['host']) && $config['connection'] = [$config['connection']];
        empty($config['logfile']) && $config['logfile'] = '/var/log/phprabbit/log.log';
        $config['retry'] = (array)($config['retry'] ?? [10000, 100000, 250000]);
        $this->config = $config;
    }

    /**
     * Получение подключения к серверу
     * @return AMQPStreamConnection
     * @throws AMQPConnectionClosedException
     */
    public function getConnection(): AMQPStreamConnection
    {
        try {
            $this->connection ?? $this->connection = AMQPStreamConnection::create_connection($this->config['connection']);
            if (!$this->isConnected()) {
                // пытаемся восстановить подключение
                $try = 0;
                $attempts = count($this->config['retry']);
                while (!($connected = $this->isConnected()) && $try < $attempts) {
                    // пытаемся переподключиться в случае потери соединения
                    usleep($this->config['retry'][$try++]);
                    $this->connection->reconnect();
                }
                if (!$connected) {
                    // не смогли подключиться
                    throw new AMQPConnectionClosedException();
                }
            }
        } catch (Throwable $e) {
            $host = $this->config['connection'][0]['host'];
            $message = $e->getMessage();
            $this->exception("Can't connect to rabbit server: $host" . (!empty($message) ? " ($message)" : ''));
        }
        return $this->connection;
    }

    /**
     * Получение канала работы с сервером
     * @return AMQPChannel
     * @throws AMQPConnectionClosedException
     */
    public function getChannel(): AMQPChannel
    {
        !isset($this->channel) && $this->channel = $this->getConnection()->channel();
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
     * @throws Exception
     */
    public function reconnect(): bool
    {
        $this->isConnected() && $this->connection->close();
        unset($this->connection, $this->channel);
        try {
            $this->getConnection();
            return true;
        } catch (AMQPConnectionClosedException $e) {
            // не смогли подключиться
            return false;
        }
    }

    /**
     * Проверка, что соединение с сервером установлено
     * @return bool
     */
    public function isConnected(): bool
    {
        return isset($this->connection) && $this->connection->isConnected();
    }

    /**
     * Выброс исключения с записью ошибки
     * @param string $message сообщение об ошибке
     * @throws AMQPConnectionClosedException
     */
    public function exception(string $message): void
    {
        $this->getLogger()->error($message);
        throw new AMQPConnectionClosedException($message);
    }

    /**
     * Получение логгера
     * @return Logger
     */
    public function getLogger(): Logger
    {
        if (!isset($this->logger)) {
            $path = dirname($this->config['logFile']);
            if (!file_exists($path) && !mkdir($path, 0777, true) && !is_dir($path)) {
                throw new RuntimeException("Directory `$path` was not created");
            }
            if (!is_dir($path) || !is_writable($path)) {
                throw new RuntimeException("Directory `$path` not writable");
            }
            $stream = (new StreamHandler($this->config['logFile']))->setFormatter(
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
     * @throws Exception
     */
    public function __destruct()
    {
        $this->isConnected() && $this->connection->close();
    }
}