<?php

declare(strict_types=1);

namespace Queue\Drivers;

use Queue\Contracts\Message;
use Queue\Contracts\QueueInterface;
use Queue\Exceptions\QueueException;

/**
 * Shared behaviour for all concrete drivers.
 */
abstract class AbstractDriver implements QueueInterface
{
    // ---------------------------------------------------------------
    // Serialization helpers
    // ---------------------------------------------------------------

    protected function serialize(mixed $payload): string
    {
        try {
            return json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
        } catch (\JsonException $e) {
            throw new QueueException('Failed to serialize message payload: ' . $e->getMessage(), 0, $e);
        }
    }

    protected function deserialize(string $raw): mixed
    {
        try {
            return json_decode($raw, associative: true, flags: JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            // Fallback: return raw string for non-JSON payloads
            return $raw;
        }
    }

    // ---------------------------------------------------------------
    // Default subscribe() loop – drivers may override for efficiency
    // ---------------------------------------------------------------

    public function subscribe(string $queue, callable $callback, array $options = []): void
    {
        $pollInterval = (int) ($options['poll_interval_ms'] ?? 200_000); // µs
        $maxMessages  = (int) ($options['max_messages']    ?? PHP_INT_MAX);
        $consumed     = 0;

        while ($consumed < $maxMessages) {
            $message = $this->consume($queue, $options);

            if ($message === null) {
                usleep($pollInterval);
                continue;
            }

            $continue = $callback($message);
            $consumed++;

            if ($continue === false) {
                break;
            }
        }
    }
}
