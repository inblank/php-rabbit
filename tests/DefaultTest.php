<?php

use inblank\rabbit\Connection;
use inblank\rabbit\Envelope;
use PHPUnit\Framework\TestCase;

/**
 * Класс тестов по умолчанию
 */
class DefaultTest extends TestCase
{
    /** @var string[][] Данные для тестовой отправки */
    protected array $messages = [
        ["data" => 'Test 1'],
        ["data" => 'Test 2'],
    ];

    /**
     * Полный тест нормальной отправки и получения
     * @throws JsonException
     */
    public function testAll(): void
    {
        $rabbit = new Connection($_ENV['config']);

        // очищаем очередь перед тестами
        $rabbit->getQueue('test')->purge();

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
        self::assertInstanceOf(Envelope::class, ($envelope = $queue->get()));

        //---------------------------------------------------
        // Прослушка
        $envelope->nack(); // возвращаем в очередь
        $processed = 0;
        $channel = $queue->consume(function (Envelope $envelope) use (&$processed) {
            $processed++;
            $envelope->ack();
        });
        $step = 0;
        while ($channel->is_consuming() && ++$step < 3) {
            $channel->wait(null, true);
            usleep(30000);
        }
        self::assertNull($queue->get());
        self::assertEquals(1, $processed);
    }
}
