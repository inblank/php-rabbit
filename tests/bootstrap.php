<?php

/**
 * Конфигурация подключения, обменников и очередей
 */

use inblank\rabbit\Exchange;

$_ENV['config'] = [
    'connection' => [
        'host' => 'rabbit',
        'port' => 5672,
        'user' => 'test',
        'password' => 'test'
    ],
    'exchanges' => [
        'test' => [
            'type' => Exchange::TYPE_DIRECT,
            'bind' => ['test']
        ],
    ],
    'queues' => ['test']
];
