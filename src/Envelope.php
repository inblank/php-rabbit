<?php

namespace inblank\rabbit;

use PhpAmqpLib\Message\AMQPMessage;
use Throwable;

/**
 * Сообщение
 */
class Envelope
{
    /** @var Queue Очередь из которой сообщение */
    private Queue $queue;

    /** @var AMQPMessage Сообщение */
    private AMQPMessage $envelope;

    /**
     * Конструктор
     * @param Queue $queue очередь из которой сообщение
     * @param AMQPMessage $envelope сообщение полученное из очереди
     */
    public function __construct(Queue $queue, AMQPMessage $envelope)
    {
        $this->queue = $queue;
        $this->envelope = $envelope;
    }

    /**
     * Получение исходного сообщения
     * @return string
     */
    public function raw(): string
    {
        return $this->envelope->getBody();
    }

    /**
     * Получение преобразованного json сообщения
     * @return mixed
     */
    public function content()
    {
        try {
            return json_decode($this->raw(), true, 512, JSON_THROW_ON_ERROR);
        } catch (Throwable $e) {
            $this->queue->getLogger()->error("Error envelop: {$this->raw()} in queue {$this->queue->getName()}");
            return null;
        }
    }

    /**
     * Подтверждение обработки сообщения
     * @return bool
     */
    public function ack(): bool
    {
        $this->envelope->ack();
        return true;
    }

    /**
     * НЕ подтверждение обработки сообщения
     * @param bool $requeue признак возврата сообщения обратно в очередь
     * @return bool
     */
    public function nack(bool $requeue = true): bool
    {
        $this->envelope->nack($requeue);
        return true;
    }
}