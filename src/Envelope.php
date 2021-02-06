<?php

namespace inblank\rabbit;

use AMQPEnvelope;
use Throwable;

/**
 * Сообщение
 */
class Envelope
{
    /**
     * Очередь
     * @var Queue
     */
    private Queue $queue;
    /**
     * Сообщение
     * @var \AMQPEnvelope|null
     */
    public ?AMQPEnvelope $envelope;

    /**
     * Конструктор
     * @param Queue $queue очередь из которой сообщение
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function __construct(Queue $queue)
    {
        $this->queue = $queue;
        $envelope = $queue->getQueue()->get();
        $this->envelope = $envelope ?: null;
    }

    /**
     * Получение преобразованного json сообщения
     * @return mixed возвращает null если сообщение не получено
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function content()
    {
        if (!$this->envelope) {
            return null;
        }
        $body = $this->envelope->getBody();
        try {
            return json_decode($body, true, 512, JSON_THROW_ON_ERROR);
        } catch (Throwable $e) {
            $this->queue->getLogger()->error("Error envelop: $body in queue {$this->queue->getQueue()->getName()}");
            return null;
        }
    }

    /**
     * Подтверждение обработки сообщения
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function ack(): bool
    {
        if (!$this->envelope) {
            return false;
        }
        return $this->queue->getQueue()->ack($this->envelope->getDeliveryTag());
    }

    /**
     * НЕ подтверждение обработки сообщения
     * @param int $flags флаги не подтверждения обработки
     * @return bool
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function nack(int $flags = AMQP_REQUEUE): bool
    {
        if (!$this->envelope) {
            return false;
        }
        return $this->queue->getQueue()->nack($this->envelope->getDeliveryTag(), $flags);
    }
}