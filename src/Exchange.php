<?php

namespace inblank\rabbit;

use Monolog\Logger;

/**
 * Обменник
 */
class Exchange
{
    /**
     * Список обменников
     * @var Exchange[]
     */
    private static array $instances = [];
    /**
     * Обменник
     * @var \AMQPExchange|null
     */
    private ?\AMQPExchange $exchange = null;
    /**
     * Имя обменника
     * @var string
     */
    private string $name;
    /**
     * Основной класс подключения к серверу
     * @var Rabbit
     */
    private Rabbit $rabbit;

    /**
     * Конструктор
     * @param Rabbit $rabbit конфигурация и подключение к серверу
     * @param string $name имя обменника в конфигурации
     */
    private function __construct(Rabbit $rabbit, string $name)
    {
        $this->name = $name;
        $this->rabbit = $rabbit;
    }

    /**
     * Получение инстанса обменника
     * @param Rabbit $rabbit конфигурация и подключение к серверу
     * @param string $name имя обменника в конфигурации
     * @return static
     */
    public static function getInstance(Rabbit $rabbit, string $name): self
    {
        $key = md5(json_encode($rabbit->config['connection']) . $name);
        if (!isset(self::$instances[$key])) {
            self::$instances[$key] = new self($rabbit, $name);
        }
        return self::$instances[$key];
    }

    /**
     * Получения обменника
     * @return \AMQPExchange
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function getExchange(): \AMQPExchange
    {
        if ($this->exchange === null) {
            // создаем новый обменник
            if (empty($this->rabbit->config['exchanges'][$this->name])) {
                $this->exception("Exchange `{$this->name}` not defined");
            }
            $config = $this->rabbit->config['exchanges'][$this->name];
            // создаем и настраиваем обменник
            $this->exchange = new \AMQPExchange($this->rabbit->getChannel());
            $this->exchange->setName($this->name);
            if (!empty($config['type'])) {
                $this->exchange->setType($config['type']);
            }
            if (!empty($config['flags'])) {
                $this->exchange->setFlags($config['flags']);
            }
            // объявляем обменник
            try {
                if (!$this->exchange->declareExchange()) {
                    // не удалось объявить обменник
                    throw new \AMQPExchangeException();
                }
            } catch (\Throwable $e) {
                // что-то пошло не так
                $message = $e->getMessage();
                $this->exception(empty($message) ? "Error declare exchange `{$this->name}`" : $message);
            }
            // создаем и биндим все нужные очереди
            foreach ($this->rabbit->config['exchanges'][$this->name]['bind'] ?? [] as $queueName => $routingKey) {
                if (is_int($queueName)) {
                    // задано без ключа роутинга
                    $queueName = $routingKey;
                    $routingKey = null;
                }
                $queue = Queue::getInstance($this->rabbit, $queueName);
                $queue->getQueue()->bind($this->name, $routingKey);
            }
        }
        return $this->exchange;
    }

    /**
     * Публикация сообщения в обменник
     * @param string $message сообщение для публикации
     * @param string|null $key ключ роутинга
     * @return bool
     */
    public function publish(string $message, ?string $key = null): bool
    {
        $reconnected = false;
        do {
            // получаем обменник
            try {
                return $this->getExchange()->publish($message, $key);
            } catch (\AMQPException $e) {
                // возможно проблемы с подключением
                if ($reconnected) {
                    // уже пытались подключиться повторно
                    return false;
                }
                // пытаемся подключиться по новой
                if (!($reconnected = $this->rabbit->reconnect())) {
                    // не смогли подключиться повторно
                    return false;
                }
                // уходим на вторую попытку отправки
                continue;
            }
        } while (true);
    }

    /**
     * Вызов исключения с записью ошибки
     * @param string $message сообщение об ошибке
     * @throws \AMQPExchangeException
     */
    public function exception(string $message): void
    {
        $this->rabbit->getLogger()->error($message);
        throw new \AMQPExchangeException($message);
    }

    /**
     * Сброс всех инициализированных обменников
     */
    public static function reset(): void
    {
        foreach (self::$instances as $instance) {
            $instance->exchange = null;
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
