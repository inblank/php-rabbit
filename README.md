# Работа с брокером сообщений

## Пример запуска

```php
<?php
use inblank\rabbit\Connection;

include_once 'vendor/autoload.php';

// 1. создаем подключение
$rabbit = new Connection([
    'connection' => [
        'host' => 'rabbit',
        'port' => 5672,
        'login' => 'login',
        'password' => 'pass'
    ],
    'exchanges' => [
        'exchange' => [
            'type' => \inblank\rabbit\Exchange::TYPE_DIRECT,
            'bind' => ['queue']
        ],
    ],
    'queues' => [
        'queue'
    ]
]);

// 2. отправка сообщения на обменник
$rabbit->getExchange('exchange')->publish(['id'=>1, 'name'=>'Name']);

// 3. получение сообщения из очереди
$message = $rabbit->getQueue('queue')->get();
if ($message) {
    // сообщение получено, получаем его содержимое
    $value = $message->content();
    if ($value['id']===1) {
        // подтверждаем обработку сообщения
        $message->ack();
    } else {
        // ... или возвращаем в обработку
        $message->nack();
    }
}
 
// 4. Прослушка (не блокирующая)
$channel = $rabbit->getQueue('queue')->consume(
    function (\inblank\rabbit\Envelope $envelope) {
        $envelope->ack();
     }
);
while ($channel->is_consuming()) {
    $channel->wait(null, true);
   // ... делаем что-то
   usleep(30000);
}

````

## Тесты

1. Запускаем тестовый rabbit
    ```bash
    $ docker-compose up -d rabbit
   ```
2. Запускаем тесты
    ```bash
   $ docker-compose run --rm php ./vendor/bin/phpunit
    ```
3. Удаляем тестовое окружение
    ```bash
   $ docker-compose down
    ```
