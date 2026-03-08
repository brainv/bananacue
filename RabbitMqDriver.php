<?php

declare(strict_types=1);

namespace Queue\Drivers;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Queue\Contracts\Message;
use Queue\Exceptions\QueueException;

/**
 * RabbitMQ Driver (AMQP 0-9-1)
 *
 * Required packages:
 *   composer require php-amqplib/php-amqplib
 *
 * Config keys:
 *   host         (string)  default 'localhost'
 *   port         (int)     default 5672
 *   user         (string)  default 'guest'
 *   password     (string)  default 'guest'
 *   vhost        (string)  default '/'
 *   exchange     (string)  default '' (direct, no exchange)
 *   exchange_type(string)  direct|fanout|topic|headers  default 'direct'
 *   durable      (bool)    default true
 *   auto_ack     (bool)    default false
 */
class RabbitMqDriver extends AbstractDriver
{
    private AMQPStreamConnection $connection;
    private AMQPChannel $channel;
    private string $exchange;
    private string $exchangeType;
    private bool $durable;

    public function __construct(array $config)
    {
        $this->connection = new AMQPStreamConnection(
            $config['host']     ?? 'localhost',
            $config['port']     ?? 5672,
            $config['user']     ?? 'guest',
            $config['password'] ?? 'guest',
            $config['vhost']    ?? '/',
        );

        $this->channel      = $this->connection->channel();
        $this->exchange     = $config['exchange']      ?? '';
        $this->exchangeType = $config['exchange_type'] ?? 'direct';
        $this->durable      = $config['durable']       ?? true;

        if ($this->exchange !== '') {
            $this->channel->exchange_declare(
                $this->exchange,
                $this->exchangeType,
                passive: false,
                durable: $this->durable,
                auto_delete: false,
            );
        }
    }

    // ---------------------------------------------------------------

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        $this->declareQueue($queue, $options);

        $properties = [
            'content_type'  => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];

        if (isset($options['priority'])) {
            $properties['priority'] = (int) $options['priority'];
        }

        if (isset($options['expiration'])) {
            $properties['expiration'] = (string) ((int) $options['expiration'] * 1000); // ms
        }

        if (!empty($options['headers'])) {
            $properties['application_headers'] = new AMQPTable($options['headers']);
        }

        $msg = new AMQPMessage($this->serialize($payload), $properties);

        $routingKey = $options['routing_key'] ?? $queue;

        $this->channel->basic_publish($msg, $this->exchange, $routingKey);

        // RabbitMQ does not return a message ID on publish without publisher confirms
        return null;
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        $this->declareQueue($queue, $options);
        $this->channel->basic_qos(0, 1, false);

        $raw = $this->channel->basic_get($queue, no_ack: false);

        if ($raw === null) {
            return null;
        }

        return $this->wrapMessage($raw, $queue);
    }

    public function subscribe(string $queue, callable $callback, array $options = []): void
    {
        $this->declareQueue($queue, $options);
        $this->channel->basic_qos(
            0,
            (int) ($options['prefetch'] ?? 1),
            false
        );

        $this->channel->basic_consume(
            $queue,
            consumer_tag: '',
            no_local:     false,
            no_ack:       (bool) ($options['auto_ack'] ?? false),
            exclusive:    false,
            nowait:       false,
            callback:     function (AMQPMessage $raw) use ($queue, $callback): void {
                $msg      = $this->wrapMessage($raw, $queue);
                $continue = $callback($msg);

                if ($continue === false) {
                    $this->channel->basic_cancel($raw->getConsumerTag());
                }
            },
        );

        while ($this->channel->is_consuming()) {
            $this->channel->wait();
        }
    }

    public function ack(Message $message): void
    {
        /** @var AMQPMessage $raw */
        $raw = $message->handle;
        $raw->ack();
    }

    public function nack(Message $message, bool $requeue = true): void
    {
        /** @var AMQPMessage $raw */
        $raw = $message->handle;
        $raw->nack($requeue);
    }

    public function purge(string $queue): void
    {
        $this->channel->queue_purge($queue);
    }

    public function size(string $queue): int
    {
        [, $msgCount] = $this->channel->queue_declare(
            $queue,
            passive: true,
        );

        return (int) $msgCount;
    }

    public function disconnect(): void
    {
        $this->channel->close();
        $this->connection->close();
    }

    // ---------------------------------------------------------------

    private function declareQueue(string $queue, array $options = []): void
    {
        $args = [];

        if (isset($options['max_priority'])) {
            $args['x-max-priority'] = ['I', (int) $options['max_priority']];
        }

        if (isset($options['dlx'])) {
            $args['x-dead-letter-exchange'] = ['S', $options['dlx']];
        }

        if (isset($options['ttl'])) {
            $args['x-message-ttl'] = ['I', (int) $options['ttl'] * 1000];
        }

        $this->channel->queue_declare(
            $queue,
            passive:     false,
            durable:     $this->durable,
            exclusive:   false,
            auto_delete: false,
            nowait:      false,
            arguments:   $args ? new AMQPTable($args) : [],
        );
    }

    private function wrapMessage(AMQPMessage $raw, string $queue): Message
    {
        $props    = $raw->get_properties();
        $envelope = $raw->getEnvelope();

        return new Message(
            payload:  $this->deserialize($raw->getBody()),
            handle:   $raw,
            queue:    $queue,
            id:       $props['message_id'] ?? null,
            meta:     [
                'routing_key'  => $envelope?->getRoutingKey(),
                'exchange'     => $envelope?->getExchange(),
                'delivery_tag' => $raw->getDeliveryTag(),
            ],
            attempts: (int) ($raw->isRedelivered() ? 1 : 0),
        );
    }
}
