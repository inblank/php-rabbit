<?php

namespace inblank\rabbit;

use Monolog\Logger;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;
use Throwable;

/**
 * Очередь
 */
class Queue
{
    /** @var Queue[] Список очередей */
    private static array $instances = [];

    /** @var Connection Основной класс подключения к серверу */
    private Connection $rabbit;

    /** @var string Имя очереди */
    private string $name;

    /**
     * Конструктор
     * @param Connection $rabbit конфигурация и подключение к серверу
     * @param string $name имя обменника в конфигурации
     */
    private function __construct(Connection $rabbit, string $name)
    {
        $this->name = $name;
        $this->rabbit = $rabbit;
        $this->declare();
    }

    /**
     * Получение канала
     * @return AMQPChannel
     */
    protected function getChannel(): AMQPChannel
    {
        return $this->rabbit->getChannel();
    }

    /**
     * Получение экземпляра очереди
     * @param Connection $rabbit конфигурация и подключение к серверу
     * @param string $name имя очереди в конфигурации
     * @return static
     */
    public static function getInstance(Connection $rabbit, string $name): self
    {
        $key = md5(serialize($rabbit->config['connection']) . $name);
        !isset(self::$instances[$key]) && (self::$instances[$key] = new self($rabbit, $name));
        return self::$instances[$key];
    }

    /**
     * Получение очереди
     * @throws RuntimeException
     */
    protected function declare(): void
    {
        if (!in_array($this->name, $this->rabbit->config['queues'], true)) {
            $this->exception("Queue `$this->name` not defined");
        }
        try {
            // создаем и настраиваем очередь
            $this->getChannel()->queue_declare(
                $this->name, false, true, false, false
            );
        } catch (Throwable $e) {
            // что-то пошло не так
            $message = $e->getMessage();
            $this->exception(empty($message) ? "Error declare queue `$this->name`" : $message);
        }
    }

    /**
     * Очистка очереди
     */
    public function purge(): self
    {
        $this->getChannel()->queue_purge($this->name);
        return $this;
    }

    /**
     * Получение сообщения из очереди
     * @return Envelope|null возвращает null если нет сообщений в очереди
     */
    public function get(): ?Envelope
    {
        $message = $this->getChannel()->basic_get($this->name);
        return !$message ? null : new Envelope($this, $message);
    }

    /**
     * Прослушивание канала
     * @param callable $callback функция для обработки сообщений. Сигнатура: function(inblank\rabbit\Envelope $envelope)
     * @param string $consumerTag имя обработчика
     * @return AMQPChannel канал на котором прослушивание
     */
    public function consume(callable $callback, string $consumerTag = ''): AMQPChannel
    {
        $channel = $this->getChannel();
        $channel->basic_consume(
            $this->name, $consumerTag, false, false, false, false,
            function (AMQPMessage $message) use ($callback) {
                $callback(new Envelope($this, $message));
            }
        );
        return $channel;
    }

    /**
     * Вызов исключения с записью ошибки
     * @param string $message сообщение об ошибке
     * @throws RuntimeException
     */
    public function exception(string $message): void
    {
        $this->getLogger()->error($message);
        throw new RuntimeException($message);
    }

    /**
     * Получение логгера
     * @return Logger
     */
    public function getLogger(): Logger
    {
        return $this->rabbit->getLogger();
    }

    /**
     * Получение имени очереди
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }
}