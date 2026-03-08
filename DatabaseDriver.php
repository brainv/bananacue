<?php

declare(strict_types=1);

namespace Queue\Drivers;

use Queue\Contracts\Message;
use Queue\Exceptions\QueueException;

/**
 * Database Driver (MySQL / PostgreSQL / SQLite via PDO)
 *
 * Uses a simple jobs table as a queue backend.
 * Suitable for low-to-medium throughput scenarios.
 *
 * Required table DDL (MySQL / MariaDB):
 * ---------------------------------------------------------------
 *   CREATE TABLE queue_jobs (
 *     id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
 *     queue         VARCHAR(255)  NOT NULL,
 *     payload       LONGTEXT      NOT NULL,
 *     attempts      TINYINT       NOT NULL DEFAULT 0,
 *     reserved_at   INT UNSIGNED  NULL,
 *     available_at  INT UNSIGNED  NOT NULL,
 *     created_at    INT UNSIGNED  NOT NULL,
 *     INDEX idx_queue_available (queue, available_at, reserved_at)
 *   );
 *
 * Config keys:
 *   dsn        (string)  PDO DSN
 *   username   (string)
 *   password   (string)
 *   table      (string)  default 'queue_jobs'
 *   reserve_for(int)     seconds a job is reserved before being retried, default 60
 */
class DatabaseDriver extends AbstractDriver
{
    private \PDO $pdo;
    private string $table;
    private int $reserveFor;

    public function __construct(array $config)
    {
        $this->pdo = new \PDO(
            $config['dsn']      ?? throw new QueueException('Database DSN is required'),
            $config['username'] ?? null,
            $config['password'] ?? null,
            [
                \PDO::ATTR_ERRMODE            => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            ],
        );

        $this->table      = $config['table']       ?? 'queue_jobs';
        $this->reserveFor = (int) ($config['reserve_for'] ?? 60);
    }

    // ---------------------------------------------------------------

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        $now          = time();
        $availableAt  = $now + (int) ($options['delay'] ?? 0);

        $stmt = $this->pdo->prepare(
            "INSERT INTO {$this->table} (queue, payload, attempts, reserved_at, available_at, created_at)
             VALUES (:queue, :payload, 0, NULL, :available_at, :created_at)"
        );

        $stmt->execute([
            ':queue'        => $queue,
            ':payload'      => $this->serialize($payload),
            ':available_at' => $availableAt,
            ':created_at'   => $now,
        ]);

        return $this->pdo->lastInsertId() ?: null;
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        $this->pdo->beginTransaction();

        try {
            $now  = time();
            $stmt = $this->pdo->prepare(
                "SELECT * FROM {$this->table}
                 WHERE queue = :queue
                   AND available_at <= :now
                   AND (reserved_at IS NULL OR reserved_at < :expired)
                 ORDER BY id ASC
                 LIMIT 1
                 FOR UPDATE SKIP LOCKED"
            );

            $stmt->execute([
                ':queue'   => $queue,
                ':now'     => $now,
                ':expired' => $now - $this->reserveFor,
            ]);

            $row = $stmt->fetch();

            if (!$row) {
                $this->pdo->rollBack();
                return null;
            }

            // Reserve the job
            $this->pdo->prepare(
                "UPDATE {$this->table}
                 SET reserved_at = :now, attempts = attempts + 1
                 WHERE id = :id"
            )->execute([':now' => $now, ':id' => $row['id']]);

            $this->pdo->commit();

            return new Message(
                payload:  $this->deserialize($row['payload']),
                handle:   $row['id'],
                queue:    $queue,
                id:       (string) $row['id'],
                meta:     ['reserved_at' => $now, 'created_at' => $row['created_at']],
                attempts: (int) $row['attempts'],
            );
        } catch (\Throwable $e) {
            $this->pdo->rollBack();
            throw new QueueException('Database queue consume failed: ' . $e->getMessage(), 0, $e);
        }
    }

    public function ack(Message $message): void
    {
        $this->pdo->prepare(
            "DELETE FROM {$this->table} WHERE id = :id"
        )->execute([':id' => $message->handle]);
    }

    public function nack(Message $message, bool $requeue = true): void
    {
        if ($requeue) {
            // Release the reservation so it's picked up again
            $this->pdo->prepare(
                "UPDATE {$this->table}
                 SET reserved_at = NULL, available_at = :now
                 WHERE id = :id"
            )->execute([':now' => time(), ':id' => $message->handle]);
        } else {
            $this->ack($message);
        }
    }

    public function purge(string $queue): void
    {
        $this->pdo->prepare(
            "DELETE FROM {$this->table} WHERE queue = :queue"
        )->execute([':queue' => $queue]);
    }

    public function size(string $queue): int
    {
        $stmt = $this->pdo->prepare(
            "SELECT COUNT(*) FROM {$this->table}
             WHERE queue = :queue AND reserved_at IS NULL AND available_at <= :now"
        );
        $stmt->execute([':queue' => $queue, ':now' => time()]);

        return (int) $stmt->fetchColumn();
    }

    public function disconnect(): void
    {
        // PDO has no explicit disconnect; setting to null closes the connection
        unset($this->pdo);
    }
}
