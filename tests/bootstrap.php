<?php

/**
 * Конфигурация подключения, обменников и очередей
 */
$_ENV['config'] = [
    'connection' => [
        'host' => 'rabbit',
        'port' => 5672,
        'login' => 'test',
        'password' => 'test'
    ],
    'exchanges' => [
        'test' => [
            'type' => AMQP_EX_TYPE_DIRECT,
            'flags' => AMQP_DURABLE,
            'bind' => [
                'test'
            ]
        ],
    ],
    'queues' => [
        'test' => [
            'flags' => AMQP_DURABLE,
        ]
    ]
];
