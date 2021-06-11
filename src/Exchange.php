<?php

namespace inblank\rabbit;

use Exception;
use JsonException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;
use Throwable;

/**
 * Обменник
 */
class Exchange
{
    /** @var string Тип обменника DIRECT */
    public const TYPE_DIRECT = AMQPExchangeType::DIRECT;

    /** @var string Тип обменника FANOUT */
    public const TYPE_FANOUT = AMQPExchangeType::FANOUT;

    /** @var string Тип обменника HEADERS */
    public const TYPE_HEADER = AMQPExchangeType::HEADERS;

    /** @var string Тип обменника TOPIC */
    public const TYPE_TOPIC = AMQPExchangeType::TOPIC;

    /** @var Exchange[] Список обменников */
    private static array $instances = [];

    /** @var Connection Основной класс подключения к серверу */
    private Connection $rabbit;

    /** @var string Имя обменника */
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
     * Получение экземпляра обменника
     * @param Connection $rabbit конфигурация и подключение к серверу
     * @param string $name имя обменника в конфигурации
     * @return static
     */
    public static function getInstance(Connection $rabbit, string $name): self
    {
        $key = md5(serialize($rabbit->config['connection']) . $name);
        !isset(self::$instances[$key]) && (self::$instances[$key] = new self($rabbit, $name));
        return self::$instances[$key];
    }

    /**
     * Получения обменника
     */
    protected function declare(): void
    {
        // создаем новый обменник
        if (empty($this->rabbit->config['exchanges'][$this->name])) {
            $this->exception("Exchange `$this->name` not defined");
        }
        $config = $this->rabbit->config['exchanges'][$this->name];
        // объявляем обменник
        try {
            $channel = $this->getChannel();
            $channel->exchange_declare($this->name, $config['type'], false, true, false);
            // создаем и связываем все нужные очереди
            foreach ($this->rabbit->config['exchanges'][$this->name]['bind'] ?? [] as $queueName => $routingKey) {
                is_int($queueName) && (list($queueName, $routingKey) = [$routingKey, '']); // задано без ключа роутинга
                $channel->queue_bind($queueName, $this->name, $routingKey);
            }
        } catch (Throwable $e) {
            // что-то пошло не так
            $message = $e->getMessage();
            $this->exception(empty($message) ? "Error declare exchange `$this->name`" : $message);
        }
    }

    /**
     * Публикация сообщения в обменник
     * @param mixed $message сообщение для публикации. Будет преобразовано в json
     * @param string|null $key ключ роутинга
     * @return bool
     * @throws JsonException
     * @throws Exception
     */
    public function publish($message, ?string $key = null): bool
    {
        $reconnected = false;
        do {
            $envelope = new AMQPMessage(
                json_encode($message, JSON_THROW_ON_ERROR),
                ['content_type' => 'application/json', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]
            );
            try {
                $this->getChannel()->basic_publish($envelope, $this->name, (string)$key);
                return true;
            } catch (AMQPChannelClosedException | AMQPConnectionClosedException | AMQPConnectionBlockedException $e) {
                // возможно проблемы с подключением
                if ($reconnected || !($reconnected = $this->rabbit->reconnect())) {
                    // уже пытались подключиться повторно млм не смогли подключиться повторно
                    return false;
                }
                // уходим на вторую попытку отправки
                continue;
            }
        } while (true);
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
     * Вызов исключения с записью ошибки
     * @param string $message сообщение об ошибке
     * @throws RuntimeException
     */
    public function exception(string $message): void
    {
        $this->rabbit->getLogger()->error($message);
        throw new RuntimeException($message);
    }
}
