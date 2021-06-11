<?php

namespace inblank\rabbit;

use AMQPQueue;
use AMQPQueueException;
use Monolog\Logger;
use Throwable;

/**
 * Очередь
 */
class Queue
{
    /**
     * Список очередей
     * @var Queue[]
     */
    private static array $instances = [];
    /**
     * Очередь
     * @var \AMQPQueue|null
     */
    private ?AMQPQueue $queue = null;
    /**
     * Имя очереди
     * @var string
     */
    private string $name;
    /**
     * Основной класс подключения к серверу
     * @var Connection
     */
    private Connection $rabbit;

    /**
     * Конструктор
     * @param Connection $rabbit конфигурация и подключение к серверу
     * @param string $name имя обменника в конфигурации
     */
    private function __construct(Connection $rabbit, string $name)
    {
        $this->name = $name;
        $this->rabbit = $rabbit;
    }

    /**
     * Получение инстанса очереди
     * @param Connection $rabbit конфигурация и подключение к серверу
     * @param string $name имя очереди в конфигурации
     * @return static
     */
    public static function getInstance(Connection $rabbit, string $name): self
    {
        $key = md5(serialize($rabbit->config['connection']) . $name);
        if (!isset(self::$instances[$key])) {
            self::$instances[$key] = new self($rabbit, $name);
        }
        return self::$instances[$key];
    }

    /**
     * Получение очереди
     * @return \AMQPQueue
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function getQueue(): AMQPQueue
    {
        if ($this->queue === null) {
            if (empty($this->rabbit->config['queues'][$this->name])) {
                $this->exception("Queue `$this->name` not defined");
            }
            // создаем и настраиваем очередь
            $config = $this->rabbit->config['queues'][$this->name];
            $this->queue = new AMQPQueue($this->rabbit->getChannel());
            $this->queue->setName($this->name);
            if (!empty($config['flags'])) {
                $this->queue->setFlags($config['flags']);
            }
            // объявляем очередь
            try {
                $this->queue->declareQueue();
            } catch (Throwable $e) {
                // что-то пошло не так
                $message = $e->getMessage();
                $this->exception(empty($message) ? "Error declare queue `$this->name`" : $message);
            }
        }
        return $this->queue;
    }

    /**
     * Получение сообщения из очереди
     * @return Envelope|null возвращает null если нет сообщений в очереди
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function get(): ?Envelope
    {
        $message = $this->getQueue()->get();
        if (!$message) {
            return null;
        }
        return new Envelope($this, $message);
    }

    /**
     * Вызов исключения с записью ошибки
     * @param string $message сообщение об ошибке
     * @throws \AMQPQueueException
     */
    public function exception(string $message): void
    {
        $this->rabbit->getLogger()->error($message);
        throw new AMQPQueueException($message);
    }

    /**
     * Сброс всех инициализированных очередей
     */
    public static function reset(): void
    {
        foreach (self::$instances as $instance) {
            $instance->queue = null;
        }
    }

    /**
     * Получение логгера
     * @return Logger
     */
    public function getLogger(): Logger
    {
        return $this->rabbit->getLogger();
    }
}