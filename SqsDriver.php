<?php

declare(strict_types=1);

namespace Queue\Drivers;

use Aws\Sqs\SqsClient;
use Queue\Contracts\Message;
use Queue\Exceptions\QueueException;

/**
 * AWS SQS Driver
 *
 * Required packages:
 *   composer require aws/aws-sdk-php
 *
 * Config keys:
 *   region      (string)  AWS region, e.g. 'us-east-1'
 *   version     (string)  SQS API version, default 'latest'
 *   credentials (array)   ['key' => '…', 'secret' => '…']  (optional – uses IAM role if omitted)
 *   queue_url   (string)  Default queue URL (can be overridden per call via $options)
 *   endpoint    (string)  Custom endpoint URL (useful for LocalStack)
 */
class SqsDriver extends AbstractDriver
{
    private SqsClient $client;
    private string $defaultQueueUrl;

    public function __construct(array $config)
    {
        $clientConfig = [
            'region'  => $config['region']  ?? 'us-east-1',
            'version' => $config['version'] ?? 'latest',
        ];

        if (isset($config['credentials'])) {
            $clientConfig['credentials'] = $config['credentials'];
        }

        if (isset($config['endpoint'])) {
            $clientConfig['endpoint'] = $config['endpoint'];
        }

        $this->client          = new SqsClient($clientConfig);
        $this->defaultQueueUrl = $config['queue_url'] ?? '';
    }

    // ---------------------------------------------------------------

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        $params = [
            'QueueUrl'    => $this->resolveUrl($queue, $options),
            'MessageBody' => $this->serialize($payload),
        ];

        // Delay in seconds (0–900)
        if (isset($options['delay'])) {
            $params['DelaySeconds'] = (int) $options['delay'];
        }

        // FIFO queues require MessageGroupId
        if (isset($options['group_id'])) {
            $params['MessageGroupId']         = $options['group_id'];
            $params['MessageDeduplicationId'] = $options['deduplication_id']
                ?? md5($params['MessageBody'] . microtime());
        }

        // Optional message attributes
        if (!empty($options['attributes'])) {
            $params['MessageAttributes'] = $options['attributes'];
        }

        $result = $this->client->sendMessage($params);

        return $result->get('MessageId');
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        $result = $this->client->receiveMessage([
            'QueueUrl'            => $this->resolveUrl($queue, $options),
            'MaxNumberOfMessages' => 1,
            'WaitTimeSeconds'     => (int) ($options['wait_time'] ?? 1),
            'VisibilityTimeout'   => (int) ($options['visibility_timeout'] ?? 30),
            'AttributeNames'      => ['All'],
        ]);

        $messages = $result->get('Messages') ?? [];

        if (empty($messages)) {
            return null;
        }

        $raw = $messages[0];

        return new Message(
            payload:  $this->deserialize($raw['Body']),
            handle:   [
                'ReceiptHandle' => $raw['ReceiptHandle'],
                'QueueUrl'      => $this->resolveUrl($queue, $options),
            ],
            queue:    $queue,
            id:       $raw['MessageId'],
            meta:     $raw['Attributes'] ?? [],
            attempts: (int) ($raw['Attributes']['ApproximateReceiveCount'] ?? 0),
        );
    }

    public function ack(Message $message): void
    {
        $this->client->deleteMessage([
            'QueueUrl'      => $message->handle['QueueUrl'],
            'ReceiptHandle' => $message->handle['ReceiptHandle'],
        ]);
    }

    public function nack(Message $message, bool $requeue = true): void
    {
        if (!$requeue) {
            // Immediately delete (let DLQ handle it if configured)
            $this->ack($message);
            return;
        }

        // Make the message visible again immediately
        $this->client->changeMessageVisibility([
            'QueueUrl'          => $message->handle['QueueUrl'],
            'ReceiptHandle'     => $message->handle['ReceiptHandle'],
            'VisibilityTimeout' => 0,
        ]);
    }

    public function purge(string $queue): void
    {
        $this->client->purgeQueue([
            'QueueUrl' => $this->resolveUrl($queue),
        ]);
    }

    public function size(string $queue): int
    {
        $result = $this->client->getQueueAttributes([
            'QueueUrl'       => $this->resolveUrl($queue),
            'AttributeNames' => ['ApproximateNumberOfMessages'],
        ]);

        return (int) ($result->get('Attributes')['ApproximateNumberOfMessages'] ?? 0);
    }

    public function disconnect(): void
    {
        // AWS SDK is stateless – nothing to close
    }

    // ---------------------------------------------------------------

    private function resolveUrl(string $queue, array $options = []): string
    {
        return $options['queue_url'] ?? (
            str_starts_with($queue, 'https://') ? $queue : $this->defaultQueueUrl
        );
    }
}
