<?php

use inblank\rabbit\Connection;
use inblank\rabbit\Envelope;
use PHPUnit\Framework\TestCase;

class DefaultTest extends TestCase
{
    /**
     * Данные для тестовой отправки
     * @var string[][]
     */
    protected array $messages = [
        ["data" => 'Test 1'],
        ["data" => 'Test 2'],
    ];

    /**
     * Полный тест нормальной отправки и получения
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     * @throws \JsonException
     */
    public function testAll(): void
    {
        $rabbit = new Connection($_ENV['config']);
        // очищаем очередь перед тестами
        $rabbit->getQueue('test')->getQueue()->purge();

        //---------------------------------------------------
        // Отправка
        $exchange = $rabbit->getExchange('test');
        self::assertTrue($exchange->publish($this->messages[0]));
        self::assertTrue($exchange->publish($this->messages[1]));

        //---------------------------------------------------
        // Получение
        $queue = $rabbit->getQueue('test');

        $envelope0 = $queue->get();
        self::assertNotNull($envelope0);
        self::assertEquals($this->messages[0], $envelope0->content());

        $envelope1 = $queue->get();
        self::assertNotNull($envelope1);
        self::assertEquals($this->messages[1], $envelope1->content());

        self::assertNull($queue->get());

        //---------------------------------------------------
        // Подтверждение
        self::assertTrue($envelope0->ack());
        self::assertNull($queue->get());

        //---------------------------------------------------
        // НЕ подтверждение
        self::assertTrue($envelope1->nack());
        self::assertInstanceOf(Envelope::class, $queue->get());
    }
}
