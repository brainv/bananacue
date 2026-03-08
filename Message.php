<?php

declare(strict_types=1);

namespace Queue\Contracts;

/**
 * Immutable value object that wraps a broker message.
 *
 * The raw $handle property carries whatever the driver needs to
 * ack/nack the message (e.g. SQS ReceiptHandle, RabbitMQ delivery_tag, etc.).
 */
final class Message
{
    public function __construct(
        /** Decoded payload (already unserialized by the driver). */
        public readonly mixed $payload,

        /** Opaque handle used by the driver for ack/nack. */
        public readonly mixed $handle,

        /** Original queue / topic name this message arrived on. */
        public readonly string $queue,

        /** Broker-assigned message identifier (null if unavailable). */
        public readonly ?string $id = null,

        /** Driver-specific metadata (headers, attributes, partition, offset…). */
        public readonly array $meta = [],

        /** How many times this message has been delivered (0 = first attempt). */
        public readonly int $attempts = 0,
    ) {}

    /**
     * Create a copy with additional metadata entries merged in.
     */
    public function withMeta(array $extra): self
    {
        return new self(
            payload:  $this->payload,
            handle:   $this->handle,
            queue:    $this->queue,
            id:       $this->id,
            meta:     array_merge($this->meta, $extra),
            attempts: $this->attempts,
        );
    }
}
