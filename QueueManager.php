<?php

declare(strict_types=1);

namespace Queue;

use Queue\Contracts\Message;
use Queue\Contracts\QueueInterface;
use Queue\Drivers\ActiveMqDriver;
use Queue\Drivers\DatabaseDriver;
use Queue\Drivers\InMemoryDriver;
use Queue\Drivers\KafkaDriver;
use Queue\Drivers\PulsarDriver;
use Queue\Drivers\RabbitMqDriver;
use Queue\Drivers\RedisDriver;
use Queue\Drivers\SqsDriver;
use Queue\Exceptions\QueueException;

/**
 * QueueManager
 *
 * Central factory + proxy class.  Configure multiple named connections and
 * switch between them with ->connection('name').
 *
 * Usage:
 * ---------------------------------------------------------------
 *   $manager = new QueueManager([
 *       'default' => 'sqs',
 *       'connections' => [
 *           'sqs' => [
 *               'driver'      => 'sqs',
 *               'region'      => 'us-east-1',
 *               'queue_url'   => 'https://sqs.us-east-1.amazonaws.com/123/my-queue',
 *               'credentials' => ['key' => '…', 'secret' => '…'],
 *           ],
 *           'rabbit' => [
 *               'driver'   => 'rabbitmq',
 *               'host'     => 'localhost',
 *               'user'     => 'guest',
 *               'password' => 'guest',
 *           ],
 *           'kafka' => [
 *               'driver'   => 'kafka',
 *               'brokers'  => 'kafka:9092',
 *               'group_id' => 'my-app',
 *           ],
 *       ],
 *   ]);
 *
 *   // Publish on the default connection
 *   $manager->publish('orders', ['order_id' => 42]);
 *
 *   // Consume on a specific connection
 *   $msg = $manager->connection('rabbit')->consume('orders');
 * ---------------------------------------------------------------
 */
class QueueManager implements QueueInterface
{
    /** @var array<string, QueueInterface> Resolved & cached driver instances. */
    private array $resolved = [];

    /** @var array<string, callable> Custom driver resolvers. */
    private array $customDrivers = [];

    private string $defaultConnection;

    /** @var array<string, array> Raw connection configs. */
    private array $connections;

    public function __construct(array $config = [])
    {
        $this->defaultConnection = $config['default'] ?? 'memory';
        $this->connections       = $config['connections'] ?? [
            'memory' => ['driver' => 'memory'],
        ];
    }

    // ---------------------------------------------------------------
    // Connection management
    // ---------------------------------------------------------------

    /**
     * Get (or create) a named driver instance.
     */
    public function connection(?string $name = null): QueueInterface
    {
        $name ??= $this->defaultConnection;

        if (!isset($this->resolved[$name])) {
            $this->resolved[$name] = $this->resolve($name);
        }

        return $this->resolved[$name];
    }

    /**
     * Register a custom driver factory.
     *
     * @param string   $name    Driver key used in config ('driver' => $name)
     * @param callable $factory fn(array $config): QueueInterface
     */
    public function extend(string $name, callable $factory): void
    {
        $this->customDrivers[$name] = $factory;
    }

    /**
     * Disconnect and remove a resolved connection.
     */
    public function forgetConnection(string $name): void
    {
        if (isset($this->resolved[$name])) {
            $this->resolved[$name]->disconnect();
            unset($this->resolved[$name]);
        }
    }

    // ---------------------------------------------------------------
    // QueueInterface proxy — delegates to the default connection
    // ---------------------------------------------------------------

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        return $this->connection()->publish($queue, $payload, $options);
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        return $this->connection()->consume($queue, $options);
    }

    public function subscribe(string $queue, callable $callback, array $options = []): void
    {
        $this->connection()->subscribe($queue, $callback, $options);
    }

    public function ack(Message $message): void
    {
        $this->connection()->ack($message);
    }

    public function nack(Message $message, bool $requeue = true): void
    {
        $this->connection()->nack($message, $requeue);
    }

    public function purge(string $queue): void
    {
        $this->connection()->purge($queue);
    }

    public function size(string $queue): int
    {
        return $this->connection()->size($queue);
    }

    public function disconnect(): void
    {
        foreach ($this->resolved as $driver) {
            $driver->disconnect();
        }

        $this->resolved = [];
    }

    // ---------------------------------------------------------------
    // Internal factory
    // ---------------------------------------------------------------

    private function resolve(string $name): QueueInterface
    {
        $config = $this->connections[$name]
            ?? throw new QueueException("Queue connection [{$name}] is not defined.");

        $driver = strtolower($config['driver'] ?? '');

        if (isset($this->customDrivers[$driver])) {
            return ($this->customDrivers[$driver])($config);
        }

        return match ($driver) {
            'sqs'                        => new SqsDriver($config),
            'rabbitmq', 'rabbit', 'amqp' => new RabbitMqDriver($config),
            'kafka'                      => new KafkaDriver($config),
            'redis'                      => new RedisDriver($config),
            'activemq', 'stomp'          => new ActiveMqDriver($config),
            'pulsar'                     => new PulsarDriver($config),
            'database', 'db'             => new DatabaseDriver($config),
            'memory', 'array', 'test'    => new InMemoryDriver(),
            default                      => throw new QueueException(
                "Unsupported queue driver [{$driver}]. " .
                "Available: sqs, rabbitmq, kafka, redis, activemq, pulsar, database, memory. " .
                "Register custom drivers via QueueManager::extend()."
            ),
        };
    }
}
