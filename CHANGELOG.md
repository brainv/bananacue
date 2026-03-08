# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] - 2026-03-08

### Added

- `QueueInterface` — universal contract covering `publish`, `consume`, `ack`, `nack`, `size`, `purge`, and `subscribe`.
- `Message` — immutable value-object carrying `payload`, `id`, `queue`, `attempts`, `meta`, and `handle`.
- `AbstractDriver` — base class providing JSON serialization/deserialization and a default `subscribe()` polling loop.
- `QueueManager` — factory and multi-connection manager with `connection()`, `extend()`, and `disconnect()` helpers.
- `QueueException` — single exception type (extends `\RuntimeException`) thrown by all drivers on unrecoverable errors.
- **SqsDriver** — AWS SQS driver with support for FIFO queues (`group_id`), message delay, and custom `MessageAttributes`. Supports LocalStack via configurable `endpoint`.
- **RabbitMqDriver** — RabbitMQ driver over AMQP 0-9-1 (`php-amqplib`). Supports durable queues, exchanges, dead-letter exchange, TTL, priority, and per-message `routing_key`.
- **KafkaDriver** — Apache Kafka driver via `ext-rdkafka`. Supports multi-broker setups, consumer groups, partition/key/header publish options, and configurable `auto_offset_reset`.
- **RedisDriver** — Redis driver using LIST for immediate jobs and ZSET for delayed scheduling (`BLPOP`-based consumption). Supports `ext-redis` and `predis/predis`.
- **ActiveMqDriver** — ActiveMQ / generic STOMP broker driver (`stomp-php`). Supports queue and topic destination types, persistent messages, priority, and expiry.
- **PulsarDriver** — Apache Pulsar driver over the HTTP REST API (`guzzlehttp/guzzle`). Supports JWT authentication, all subscription types (Exclusive, Shared, Failover, Key_Shared), and millisecond-precision delivery delay.
- **DatabaseDriver** — PDO-based driver compatible with MySQL, PostgreSQL, and SQLite. Includes optimistic reservation with configurable `reserve_for` timeout to handle crashed workers and delayed job publish.
- **InMemoryDriver** — in-process driver backed by a plain PHP array. Intended for unit and integration tests; zero external dependencies.
- Custom driver registration via `QueueManager::extend()`.
- PHPUnit 10 test suite covering `Message`, `QueueException`, `QueueManager`, `AbstractDriver`, and `InMemoryDriver`.

[1.0.0]: https://github.com/brainv/bananacue/releases/tag/v1.0.0
