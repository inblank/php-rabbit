<?php

use inblank\rabbit\Rabbit;
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
     */
    public function testAll(): void
    {
        $rabbit = new Rabbit($_ENV['config']);
        // очищаем очередь перед тестами
        $rabbit->getQueue('test')->getQueue()->purge();

        //---------------------------------------------------
        // Отправка
        $exchange = $rabbit->getExchange('test');
        self::assertTrue($exchange->publish(json_encode($this->messages[0])));
        self::assertTrue($exchange->publish(json_encode($this->messages[1])));

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
        self::assertInstanceOf(\inblank\rabbit\Envelope::class, $queue->get());
    }
}
