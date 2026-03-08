<?php

declare(strict_types=1);

namespace Queue\Contracts;

/**
 * Universal Queue Interface
 * Abstracts common operations across all queue backends.
 */
interface QueueInterface
{
    /**
     * Publish a message to a queue/topic.
     *
     * @param string $queue   Queue or topic name
     * @param mixed  $payload Message payload (will be serialized)
     * @param array  $options Driver-specific options (delay, priority, headers, etc.)
     *
     * @return string|null Message ID assigned by the broker (if available)
     */
    public function publish(string $queue, mixed $payload, array $options = []): ?string;

    /**
     * Consume the next available message from a queue.
     * Returns null when no message is available.
     *
     * @param string $queue   Queue name
     * @param array  $options Driver-specific options (visibility timeout, etc.)
     */
    public function consume(string $queue, array $options = []): ?Message;

    /**
     * Subscribe a callback to continuously receive messages.
     * Blocks until $callback returns false or an exception is thrown.
     *
     * @param string   $queue    Queue or topic name
     * @param callable $callback fn(Message $msg): bool  — return false to stop
     * @param array    $options  Driver-specific options
     */
    public function subscribe(string $queue, callable $callback, array $options = []): void;

    /**
     * Acknowledge that a message has been successfully processed.
     */
    public function ack(Message $message): void;

    /**
     * Negatively acknowledge a message (return it to the queue / dead-letter it).
     *
     * @param bool $requeue Whether the broker should re-enqueue the message
     */
    public function nack(Message $message, bool $requeue = true): void;

    /**
     * Delete / purge all messages from a queue.
     */
    public function purge(string $queue): void;

    /**
     * Return the approximate number of messages waiting in the queue.
     */
    public function size(string $queue): int;

    /**
     * Close the underlying connection gracefully.
     */
    public function disconnect(): void;
}
