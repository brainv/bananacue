<?php

declare(strict_types=1);

namespace Queue\Drivers;

use Queue\Contracts\Message;
use Queue\Exceptions\QueueException;

/**
 * Apache Kafka Driver
 *
 * Required packages:
 *   composer require arnaud-lb/php-rdkafka
 *   (requires librdkafka installed on the OS)
 *
 * Config keys:
 *   brokers          (string)  Comma-separated list, e.g. 'kafka1:9092,kafka2:9092'
 *   group_id         (string)  Consumer group ID
 *   auto_offset_reset(string)  'earliest' | 'latest'  (default 'earliest')
 *   producer_config  (array)   Extra librdkafka producer settings
 *   consumer_config  (array)   Extra librdkafka consumer settings
 *   timeout_ms       (int)     Poll timeout in ms (default 1000)
 */
class KafkaDriver extends AbstractDriver
{
    private \RdKafka\Producer $producer;
    private \RdKafka\KafkaConsumer $consumer;
    private int $timeoutMs;
    private array $subscribedTopics = [];

    public function __construct(array $config)
    {
        if (!extension_loaded('rdkafka')) {
            throw new QueueException('ext-rdkafka is not loaded. Install librdkafka and the PHP extension.');
        }

        $this->timeoutMs = (int) ($config['timeout_ms'] ?? 1000);

        // ----- Producer -----
        $pConf = new \RdKafka\Conf();
        $pConf->set('metadata.broker.list', $config['brokers'] ?? 'localhost:9092');

        foreach ($config['producer_config'] ?? [] as $k => $v) {
            $pConf->set($k, $v);
        }

        $this->producer = new \RdKafka\Producer($pConf);

        // ----- Consumer -----
        $cConf = new \RdKafka\Conf();
        $cConf->set('metadata.broker.list', $config['brokers'] ?? 'localhost:9092');
        $cConf->set('group.id',             $config['group_id'] ?? 'php-queue-consumer');
        $cConf->set('auto.offset.reset',    $config['auto_offset_reset'] ?? 'earliest');
        $cConf->set('enable.auto.commit',   'false'); // we handle ack manually

        foreach ($config['consumer_config'] ?? [] as $k => $v) {
            $cConf->set($k, $v);
        }

        $this->consumer = new \RdKafka\KafkaConsumer($cConf);
    }

    // ---------------------------------------------------------------

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        $topic = $this->producer->newTopic($queue);

        $partition = $options['partition'] ?? RD_KAFKA_PARTITION_UA;
        $msgFlags  = $options['msg_flags'] ?? 0;
        $key       = $options['key']       ?? null;
        $headers   = $options['headers']   ?? [];

        $topic->producev(
            $partition,
            $msgFlags,
            $this->serialize($payload),
            $key,
            $headers,
        );

        $this->producer->poll(0);

        // Flush within $timeout ms; returns RD_KAFKA_RESP_ERR__TIMED_OUT if slow
        $this->producer->flush(10_000);

        // Kafka doesn't return a stable per-message ID synchronously
        return null;
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        $this->ensureSubscribed([$queue]);

        $raw = $this->consumer->consume($this->timeoutMs);

        if ($raw === null) {
            return null;
        }

        switch ($raw->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return $this->wrapMessage($raw, $queue);

            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                return null;

            default:
                throw new QueueException('Kafka error: ' . $raw->errstr());
        }
    }

    public function subscribe(string $queue, callable $callback, array $options = []): void
    {
        $this->ensureSubscribed([$queue]);

        $maxMessages = (int) ($options['max_messages'] ?? PHP_INT_MAX);
        $consumed    = 0;

        while ($consumed < $maxMessages) {
            $raw = $this->consumer->consume($this->timeoutMs);

            if ($raw === null) {
                continue;
            }

            switch ($raw->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $msg      = $this->wrapMessage($raw, $queue);
                    $continue = $callback($msg);
                    $consumed++;

                    if ($continue === false) {
                        return;
                    }
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    break;

                default:
                    throw new QueueException('Kafka consumer error: ' . $raw->errstr());
            }
        }
    }

    /**
     * Commit the offset for this message (Kafka's equivalent of ack).
     */
    public function ack(Message $message): void
    {
        /** @var \RdKafka\Message $raw */
        $raw = $message->handle;
        $this->consumer->commit($raw);
    }

    /**
     * Kafka has no built-in nack; we simply do NOT commit the offset.
     * The message will be re-delivered on the next poll (if requeue=true).
     */
    public function nack(Message $message, bool $requeue = true): void
    {
        // No-op: offset is not committed, so the message will be retried.
    }

    /**
     * Kafka topics cannot be "purged" via the consumer API.
     * Use the admin API or CLI tools instead.
     */
    public function purge(string $queue): void
    {
        throw new QueueException(
            'Kafka does not support topic purge via the consumer API. ' .
            'Use the Kafka Admin CLI or Admin client to delete/recreate the topic.'
        );
    }

    public function size(string $queue): int
    {
        // Approximate: sum of (high watermark - low watermark) across partitions
        $meta       = $this->consumer->getMetadata(false, null, $this->timeoutMs);
        $totalLag   = 0;

        foreach ($meta->getTopics() as $topic) {
            if ($topic->getTopic() !== $queue) {
                continue;
            }

            foreach ($topic->getPartitions() as $partition) {
                [$low, $high] = $this->consumer->queryWatermarkOffsets(
                    $queue,
                    $partition->getId(),
                    $this->timeoutMs,
                );
                $totalLag += max(0, $high - $low);
            }
        }

        return $totalLag;
    }

    public function disconnect(): void
    {
        // librdkafka handles cleanup automatically; optionally unsubscribe
        if (!empty($this->subscribedTopics)) {
            $this->consumer->unsubscribe();
            $this->subscribedTopics = [];
        }
    }

    // ---------------------------------------------------------------

    private function ensureSubscribed(array $topics): void
    {
        sort($topics);

        if ($topics !== $this->subscribedTopics) {
            $this->consumer->subscribe($topics);
            $this->subscribedTopics = $topics;
        }
    }

    private function wrapMessage(\RdKafka\Message $raw, string $queue): Message
    {
        return new Message(
            payload:  $this->deserialize($raw->payload),
            handle:   $raw,
            queue:    $queue,
            id:       null,
            meta:     [
                'partition' => $raw->partition,
                'offset'    => $raw->offset,
                'key'       => $raw->key,
                'headers'   => $raw->headers ?? [],
                'timestamp' => $raw->timestamp,
            ],
            attempts: 0,
        );
    }
}
