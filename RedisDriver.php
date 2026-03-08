<?php

declare(strict_types=1);

namespace Queue\Drivers;

use Queue\Contracts\Message;
use Queue\Exceptions\QueueException;

/**
 * Redis Driver
 *
 * Uses Redis LIST commands (RPUSH / BLPOP) for a simple, reliable queue.
 * Supports delayed messages via a ZSET (sorted set) scheduler.
 *
 * Required packages (choose one):
 *   composer require predis/predis
 *   OR install ext-redis (phpredis C extension)
 *
 * Config keys:
 *   host          (string)  default 'localhost'
 *   port          (int)     default 6379
 *   password      (string|null)
 *   database      (int)     default 0
 *   timeout       (int)     connection timeout in seconds, default 2
 *   block_timeout (int)     BLPOP timeout in seconds, default 1
 *   prefix        (string)  key prefix, default 'queue:'
 *   use_predis    (bool)    force Predis even when ext-redis is available, default false
 */
class RedisDriver extends AbstractDriver
{
    /** @var \Redis|\Predis\Client */
    private object $redis;

    private string $prefix;
    private int $blockTimeout;

    public function __construct(array $config)
    {
        $this->prefix       = $config['prefix']       ?? 'queue:';
        $this->blockTimeout = (int) ($config['block_timeout'] ?? 1);

        $usePredis = ($config['use_predis'] ?? false) || !extension_loaded('redis');

        if ($usePredis) {
            if (!class_exists(\Predis\Client::class)) {
                throw new QueueException('Neither ext-redis nor predis/predis is available.');
            }

            $this->redis = new \Predis\Client([
                'scheme'   => 'tcp',
                'host'     => $config['host']     ?? 'localhost',
                'port'     => $config['port']     ?? 6379,
                'password' => $config['password'] ?? null,
                'database' => $config['database'] ?? 0,
                'timeout'  => $config['timeout']  ?? 2,
            ]);
        } else {
            $r = new \Redis();
            $r->connect(
                $config['host']    ?? 'localhost',
                $config['port']    ?? 6379,
                $config['timeout'] ?? 2,
            );

            if (!empty($config['password'])) {
                $r->auth($config['password']);
            }

            $r->select($config['database'] ?? 0);
            $this->redis = $r;
        }
    }

    // ---------------------------------------------------------------

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        $id  = $options['id'] ?? bin2hex(random_bytes(16));
        $msg = json_encode(['id' => $id, 'payload' => $this->serialize($payload)]);

        $delay = (int) ($options['delay'] ?? 0);

        if ($delay > 0) {
            // Store in a ZSET with score = Unix timestamp when it should become visible
            $this->redis->zadd($this->key($queue . ':delayed'), [
                $msg => time() + $delay,
            ]);
        } else {
            $this->redis->rpush($this->key($queue), [$msg]);
        }

        return $id;
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        // Move any matured delayed messages first
        $this->migrateDelayed($queue);

        $result = $this->redis->blpop([$this->key($queue)], $this->blockTimeout);

        if (empty($result)) {
            return null;
        }

        // ext-redis returns [$key, $value]; Predis returns [$key, $value] too
        $raw = is_array($result) ? $result[1] : $result;

        return $this->wrapRaw($raw, $queue);
    }

    public function ack(Message $message): void
    {
        // Message was already popped from the list — nothing to do.
        // If you need reliable delivery, store messages in a processing set first.
    }

    public function nack(Message $message, bool $requeue = true): void
    {
        if ($requeue) {
            // Push back to the front of the queue
            $payload = json_encode([
                'id'      => $message->id,
                'payload' => $this->serialize($message->payload),
            ]);
            $this->redis->lpush($this->key($message->queue), [$payload]);
        }
        // If !$requeue, just drop it (it's already been consumed)
    }

    public function purge(string $queue): void
    {
        $this->redis->del($this->key($queue));
        $this->redis->del($this->key($queue . ':delayed'));
    }

    public function size(string $queue): int
    {
        $this->migrateDelayed($queue);
        return (int) $this->redis->llen($this->key($queue));
    }

    public function disconnect(): void
    {
        if ($this->redis instanceof \Redis) {
            $this->redis->close();
        }
        // Predis closes automatically
    }

    // ---------------------------------------------------------------

    private function key(string $queue): string
    {
        return $this->prefix . $queue;
    }

    private function wrapRaw(string $raw, string $queue): Message
    {
        $data = json_decode($raw, true);

        return new Message(
            payload:  $this->deserialize($data['payload'] ?? $raw),
            handle:   $raw,
            queue:    $queue,
            id:       $data['id'] ?? null,
            meta:     [],
            attempts: 0,
        );
    }

    /**
     * Move delayed messages whose score (timestamp) is in the past to the main list.
     */
    private function migrateDelayed(string $queue): void
    {
        $delayedKey = $this->key($queue . ':delayed');

        // Get all messages with score <= now
        $due = $this->redis->zrangebyscore($delayedKey, '-inf', (string) time());

        foreach ($due as $msg) {
            $this->redis->rpush($this->key($queue), [$msg]);
            $this->redis->zrem($delayedKey, $msg);
        }
    }
}
