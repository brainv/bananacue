<?php

declare(strict_types=1);

namespace Queue\Drivers;

use Queue\Contracts\Message;
use Queue\Exceptions\QueueException;

/**
 * ActiveMQ Driver (via STOMP protocol)
 *
 * Required packages:
 *   composer require stomp-php/stomp-php
 *
 * Config keys:
 *   broker_uri   (string)  STOMP broker URI, e.g. 'tcp://localhost:61613'
 *   login        (string)  default ''
 *   passcode     (string)  default ''
 *   vhost        (string)  STOMP virtual host header, default '/'
 *   read_timeout (float)   seconds, default 0.5
 *   destination_type (string) 'queue' | 'topic'  default 'queue'
 */
class ActiveMqDriver extends AbstractDriver
{
    private \Stomp\StatefulStomp $stomp;
    private string $destinationType;
    private ?string $subscriptionId = null;

    public function __construct(array $config)
    {
        if (!class_exists(\Stomp\Client::class)) {
            throw new QueueException('stomp-php/stomp-php is not installed. Run: composer require stomp-php/stomp-php');
        }

        $client = new \Stomp\Client($config['broker_uri'] ?? 'tcp://localhost:61613');

        $client->setLogin(
            $config['login']    ?? '',
            $config['passcode'] ?? '',
        );

        $client->setVhostname($config['vhost'] ?? '/');

        $client->setReadTimeout(
            (int) floor($config['read_timeout'] ?? 0.5),
            (int) (fmod($config['read_timeout'] ?? 0.5, 1) * 1_000_000),
        );

        $client->connect();

        $this->stomp           = new \Stomp\StatefulStomp($client);
        $this->destinationType = $config['destination_type'] ?? 'queue';
    }

    // ---------------------------------------------------------------

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        $destination = $this->destination($queue);

        $headers = array_merge(
            ['content-type' => 'application/json'],
            $options['headers'] ?? [],
        );

        if (isset($options['persistent'])) {
            $headers['persistent'] = $options['persistent'] ? 'true' : 'false';
        }

        if (isset($options['priority'])) {
            $headers['priority'] = (string) $options['priority'];
        }

        if (isset($options['expires'])) {
            $headers['expires'] = (string) ((time() + (int) $options['expires']) * 1000);
        }

        $this->stomp->send($destination, $this->serialize($payload), $headers);

        return null;
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        $this->ensureSubscribed($queue, $options);

        $frame = $this->stomp->read();

        if ($frame === false || $frame === null) {
            return null;
        }

        return $this->wrapFrame($frame, $queue);
    }

    public function subscribe(string $queue, callable $callback, array $options = []): void
    {
        $this->ensureSubscribed($queue, $options);

        $maxMessages = (int) ($options['max_messages'] ?? PHP_INT_MAX);
        $consumed    = 0;

        while ($consumed < $maxMessages) {
            $frame = $this->stomp->read();

            if ($frame === false || $frame === null) {
                usleep(100_000);
                continue;
            }

            $msg      = $this->wrapFrame($frame, $queue);
            $continue = $callback($msg);
            $consumed++;

            if ($continue === false) {
                break;
            }
        }
    }

    public function ack(Message $message): void
    {
        /** @var \Stomp\Transport\Frame $frame */
        $frame = $message->handle;
        $this->stomp->ack($frame);
    }

    public function nack(Message $message, bool $requeue = true): void
    {
        /** @var \Stomp\Transport\Frame $frame */
        $frame = $message->handle;
        $this->stomp->nack($frame);
    }

    public function purge(string $queue): void
    {
        // STOMP protocol doesn't expose a purge command;
        // drain the queue by consuming all messages rapidly.
        $this->ensureSubscribed($queue, []);

        while (true) {
            $frame = $this->stomp->read();
            if ($frame === false || $frame === null) {
                break;
            }
            $this->stomp->ack($frame);
        }
    }

    public function size(string $queue): int
    {
        throw new QueueException(
            'ActiveMQ STOMP protocol does not expose queue depth. ' .
            'Use the ActiveMQ REST API or JMX console instead.'
        );
    }

    public function disconnect(): void
    {
        $this->stomp->getClient()->disconnect();
    }

    // ---------------------------------------------------------------

    private function destination(string $queue): string
    {
        return '/' . $this->destinationType . '/' . ltrim($queue, '/');
    }

    private function ensureSubscribed(string $queue, array $options): void
    {
        if ($this->subscriptionId === null) {
            $headers = [];

            if (($options['ack_mode'] ?? 'client') !== 'auto') {
                $headers['ack'] = $options['ack_mode'] ?? 'client';
            }

            $this->subscriptionId = $this->stomp->subscribe(
                $this->destination($queue),
                $headers,
            );
        }
    }

    private function wrapFrame(\Stomp\Transport\Frame $frame, string $queue): Message
    {
        return new Message(
            payload:  $this->deserialize($frame->body),
            handle:   $frame,
            queue:    $queue,
            id:       $frame->getHeader('message-id'),
            meta:     $frame->getHeaders(),
            attempts: 0,
        );
    }
}
