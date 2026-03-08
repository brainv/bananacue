<?php

declare(strict_types=1);

namespace Queue\Drivers;

use Queue\Contracts\Message;

/**
 * In-Memory Driver
 *
 * No external dependency required.
 * Ideal for unit/integration tests and local development.
 * Data is lost when the process exits.
 */
class InMemoryDriver extends AbstractDriver
{
    /** @var array<string, list<array{payload:mixed,id:string,meta:array}>> */
    private array $queues = [];

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        $id = $options['id'] ?? bin2hex(random_bytes(8));

        $this->queues[$queue][] = [
            'payload' => $payload,
            'id'      => $id,
            'meta'    => $options['meta'] ?? [],
        ];

        return $id;
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        if (empty($this->queues[$queue])) {
            return null;
        }

        $raw = array_shift($this->queues[$queue]);

        return new Message(
            payload:  $raw['payload'],
            handle:   $raw['id'],
            queue:    $queue,
            id:       $raw['id'],
            meta:     $raw['meta'],
            attempts: 0,
        );
    }

    public function ack(Message $message): void
    {
        // Already removed from the queue on consume — nothing to do.
    }

    public function nack(Message $message, bool $requeue = true): void
    {
        if ($requeue) {
            array_unshift($this->queues[$message->queue], [
                'payload' => $message->payload,
                'id'      => $message->id ?? bin2hex(random_bytes(8)),
                'meta'    => $message->meta,
            ]);
        }
    }

    public function purge(string $queue): void
    {
        $this->queues[$queue] = [];
    }

    public function size(string $queue): int
    {
        return count($this->queues[$queue] ?? []);
    }

    public function disconnect(): void
    {
        $this->queues = [];
    }

    /** Inspect the internal state (useful in tests). */
    public function all(string $queue): array
    {
        return $this->queues[$queue] ?? [];
    }
}
