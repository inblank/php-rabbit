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
            'type' => AMQP_EX_TYPE_DIRECT,
            'flags' => AMQP_DURABLE,
            'bind' => ['queue']
        ],
    ],
    'queues' => [
        'queue' => ['flags' => AMQP_DURABLE,]
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
```

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
