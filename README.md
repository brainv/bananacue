# BananaCue — PHP Universal Queue Abstraction Layer

A single, consistent interface for the most popular message brokers and queuing
backends — AWS SQS, RabbitMQ, Apache Kafka, Redis, ActiveMQ, Apache Pulsar,
relational databases (via PDO), and an in-memory driver for tests.

---

## File structure

```
queue/
├── Contracts/
│   ├── QueueInterface.php   # The universal interface
│   └── Message.php          # Immutable message value-object
├── Drivers/
│   ├── AbstractDriver.php   # Shared serialization + default subscribe() loop
│   ├── SqsDriver.php        # AWS SQS
│   ├── RabbitMqDriver.php   # RabbitMQ (AMQP 0-9-1)
│   ├── KafkaDriver.php      # Apache Kafka (ext-rdkafka)
│   ├── RedisDriver.php      # Redis LIST + ZSET scheduler
│   ├── ActiveMqDriver.php   # ActiveMQ / any STOMP broker
│   ├── PulsarDriver.php     # Apache Pulsar (HTTP REST API)
│   ├── DatabaseDriver.php   # MySQL / PostgreSQL / SQLite (PDO)
│   └── InMemoryDriver.php   # In-process (testing)
├── Exceptions/
│   └── QueueException.php
├── QueueManager.php         # Factory + multi-connection manager
└── composer.json
```

---

## Installation

```bash
composer require brainv/bananacue
```

Install the package(s) for your chosen driver:

| Driver      | Package                              |
|-------------|--------------------------------------|
| SQS         | `aws/aws-sdk-php`                    |
| RabbitMQ    | `php-amqplib/php-amqplib`            |
| Kafka       | `ext-rdkafka` (OS + PHP extension)   |
| Redis       | `ext-redis` OR `predis/predis`       |
| ActiveMQ    | `stomp-php/stomp-php`                |
| Pulsar      | `guzzlehttp/guzzle`                  |
| Database    | `ext-pdo` (built into PHP)           |
| In-Memory   | *(none)*                             |

---

## Quick Start

```php
use Queue\QueueManager;

$manager = new QueueManager([
    'default' => 'redis',
    'connections' => [
        'redis' => [
            'driver'   => 'redis',
            'host'     => 'localhost',
            'port'     => 6379,
        ],
        'sqs' => [
            'driver'    => 'sqs',
            'region'    => 'us-east-1',
            'queue_url' => 'https://sqs.us-east-1.amazonaws.com/123456789/my-queue',
            // credentials omitted → uses IAM role / env vars
        ],
    ],
]);

// Publish to the default (redis) connection
$manager->publish('orders', ['order_id' => 42, 'total' => 99.99]);

// Publish to a specific connection
$manager->connection('sqs')->publish('notifications', ['user_id' => 7]);

// Consume a single message
$msg = $manager->consume('orders');
if ($msg) {
    echo $msg->payload['order_id']; // 42
    $manager->ack($msg);
}

// Stream messages with a callback
$manager->subscribe('orders', function (\Queue\Contracts\Message $msg) use ($manager): bool {
    processOrder($msg->payload);
    $manager->ack($msg);
    return true; // return false to stop the loop
});

// Queue stats
echo $manager->size('orders'); // e.g. 5

// Purge a queue
$manager->purge('orders');

// Close all connections
$manager->disconnect();
```

---

## Driver Configuration Reference

### AWS SQS
```php
'sqs' => [
    'driver'      => 'sqs',
    'region'      => 'us-east-1',
    'version'     => 'latest',
    'queue_url'   => 'https://sqs.us-east-1.amazonaws.com/ACCOUNT/QUEUE',
    'credentials' => ['key' => 'AKID…', 'secret' => '…'], // optional
    'endpoint'    => 'http://localhost:4566',               // optional: LocalStack
]
```

Publish options:
- `delay` (int) – Seconds before the message is visible (0–900)
- `group_id` (string) – Required for FIFO queues
- `attributes` (array) – SQS MessageAttributes

---

### RabbitMQ
```php
'rabbit' => [
    'driver'        => 'rabbitmq',
    'host'          => 'localhost',
    'port'          => 5672,
    'user'          => 'guest',
    'password'      => 'guest',
    'vhost'         => '/',
    'exchange'      => '',          // '' = default direct exchange
    'exchange_type' => 'direct',
    'durable'       => true,
]
```

Publish options: `priority`, `expiration` (seconds), `headers`, `routing_key`

Queue declare options: `max_priority`, `dlx` (dead-letter exchange), `ttl`

---

### Apache Kafka
```php
'kafka' => [
    'driver'             => 'kafka',
    'brokers'            => 'kafka1:9092,kafka2:9092',
    'group_id'           => 'my-consumer-group',
    'auto_offset_reset'  => 'earliest',  // or 'latest'
    'timeout_ms'         => 1000,
    'producer_config'    => [],          // extra librdkafka settings
    'consumer_config'    => [],
]
```

Publish options: `partition`, `key`, `headers`

---

### Redis
```php
'redis' => [
    'driver'        => 'redis',
    'host'          => 'localhost',
    'port'          => 6379,
    'password'      => null,
    'database'      => 0,
    'prefix'        => 'queue:',
    'block_timeout' => 1,     // BLPOP wait in seconds
]
```

Publish options: `delay` (seconds), `id`

---

### ActiveMQ (STOMP)
```php
'activemq' => [
    'driver'           => 'activemq',
    'broker_uri'       => 'tcp://localhost:61613',
    'login'            => '',
    'passcode'         => '',
    'vhost'            => '/',
    'read_timeout'     => 0.5,
    'destination_type' => 'queue',  // 'queue' or 'topic'
]
```

Publish options: `persistent`, `priority`, `expires` (seconds), `headers`

---

### Apache Pulsar
```php
'pulsar' => [
    'driver'       => 'pulsar',
    'base_url'     => 'http://localhost:8080',
    'tenant'       => 'public',
    'namespace'    => 'default',
    'subscription' => 'php-sub',
    'sub_type'     => 'Shared',   // Exclusive|Shared|Failover|Key_Shared
    'token'        => null,       // JWT bearer token
]
```

Publish options: `delay` (ms), `key`, `properties`

---

### Database (PDO)
```php
'db' => [
    'driver'     => 'database',
    'dsn'        => 'mysql:host=localhost;dbname=myapp',
    'username'   => 'root',
    'password'   => 'secret',
    'table'      => 'queue_jobs',
    'reserve_for'=> 60,   // seconds before unreserved job is retried
]
```

Required DDL:
```sql
CREATE TABLE queue_jobs (
    id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    queue        VARCHAR(255) NOT NULL,
    payload      LONGTEXT     NOT NULL,
    attempts     TINYINT      NOT NULL DEFAULT 0,
    reserved_at  INT UNSIGNED NULL,
    available_at INT UNSIGNED NOT NULL,
    created_at   INT UNSIGNED NOT NULL,
    INDEX idx_queue_available (queue, available_at, reserved_at)
);
```

Publish options: `delay` (seconds)

---

### In-Memory (tests)
```php
'test' => [
    'driver' => 'memory',
]
```

---

## Registering a Custom Driver

```php
$manager->extend('ironmq', function (array $config): \Queue\Contracts\QueueInterface {
    return new IronMqDriver($config);
});
```

---

## Message Object

```php
$msg->payload   // mixed  – deserialized message body
$msg->id        // ?string – broker-assigned message ID
$msg->queue     // string  – queue / topic name
$msg->attempts  // int     – delivery attempts (0 = first)
$msg->meta      // array   – driver-specific metadata
$msg->handle    // mixed   – opaque ack handle (internal use)
```

---

## Error Handling

All drivers throw `Queue\Exceptions\QueueException` (extends `\RuntimeException`)
on unrecoverable errors.

```php
use Queue\Exceptions\QueueException;

try {
    $msg = $manager->consume('orders');
} catch (QueueException $e) {
    logger()->error('Queue error', ['message' => $e->getMessage()]);
}
```
