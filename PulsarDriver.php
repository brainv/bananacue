<?php

declare(strict_types=1);

namespace Queue\Drivers;

use Queue\Contracts\Message;
use Queue\Exceptions\QueueException;

/**
 * Apache Pulsar Driver (HTTP REST API)
 *
 * Uses Pulsar's HTTP Admin & Producer/Consumer REST API so no native extension
 * is required.  For high-throughput production use, consider the binary protocol
 * via the pulsar-client-php extension or Guzzle-based SDK.
 *
 * Required packages:
 *   composer require guzzlehttp/guzzle
 *
 * Config keys:
 *   base_url        (string)  Pulsar HTTP service URL, e.g. 'http://localhost:8080'
 *   tenant          (string)  default 'public'
 *   namespace       (string)  default 'default'
 *   subscription    (string)  Consumer subscription name, default 'php-subscription'
 *   sub_type        (string)  Exclusive|Shared|Failover|Key_Shared  default 'Shared'
 *   token           (string|null) JWT bearer token
 *   timeout         (int)     HTTP timeout in seconds, default 30
 */
class PulsarDriver extends AbstractDriver
{
    private \GuzzleHttp\Client $http;
    private string $tenant;
    private string $namespace;
    private string $subscription;
    private string $subType;

    public function __construct(array $config)
    {
        if (!class_exists(\GuzzleHttp\Client::class)) {
            throw new QueueException('guzzlehttp/guzzle is not installed. Run: composer require guzzlehttp/guzzle');
        }

        $headers = ['Content-Type' => 'application/json'];

        if (!empty($config['token'])) {
            $headers['Authorization'] = 'Bearer ' . $config['token'];
        }

        $this->http = new \GuzzleHttp\Client([
            'base_uri' => rtrim($config['base_url'] ?? 'http://localhost:8080', '/') . '/',
            'timeout'  => $config['timeout'] ?? 30,
            'headers'  => $headers,
        ]);

        $this->tenant       = $config['tenant']       ?? 'public';
        $this->namespace    = $config['namespace']     ?? 'default';
        $this->subscription = $config['subscription']  ?? 'php-subscription';
        $this->subType      = $config['sub_type']      ?? 'Shared';
    }

    // ---------------------------------------------------------------

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        $url  = $this->topicUrl($queue) . '/messages';
        $body = ['payload' => base64_encode($this->serialize($payload))];

        if (isset($options['delay'])) {
            $body['deliverAfter'] = ((int) $options['delay']) . 'ms' ;
        }

        if (isset($options['key'])) {
            $body['key'] = $options['key'];
        }

        if (!empty($options['properties'])) {
            $body['properties'] = $options['properties'];
        }

        $response = $this->http->post($url, ['json' => $body]);
        $data     = json_decode((string) $response->getBody(), true);

        return $data['messageId'] ?? null;
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        $url = $this->consumerUrl($queue) . '/receive';

        try {
            $response = $this->http->get($url, [
                'query' => ['timeout' => $options['timeout_ms'] ?? 1000],
            ]);
        } catch (\GuzzleHttp\Exception\ClientException $e) {
            if ($e->getCode() === 204 || $e->getCode() === 404) {
                return null; // No message available
            }
            throw new QueueException('Pulsar consume error: ' . $e->getMessage(), 0, $e);
        }

        if ($response->getStatusCode() === 204) {
            return null;
        }

        $data = json_decode((string) $response->getBody(), true);

        return new Message(
            payload:  $this->deserialize(base64_decode($data['payload'] ?? '')),
            handle:   $data['messageId'],
            queue:    $queue,
            id:       $data['messageId'] ?? null,
            meta:     [
                'publishTime' => $data['publishTime'] ?? null,
                'properties'  => $data['properties']  ?? [],
                'key'         => $data['key']          ?? null,
            ],
            attempts: (int) ($data['redeliveryCount'] ?? 0),
        );
    }

    public function ack(Message $message): void
    {
        $url = $this->consumerUrl($message->queue) . '/acknowledge';

        $this->http->post($url, [
            'json' => ['messageId' => $message->handle],
        ]);
    }

    public function nack(Message $message, bool $requeue = true): void
    {
        if ($requeue) {
            $url = $this->consumerUrl($message->queue) . '/negativeAcknowledge';
            $this->http->post($url, [
                'json' => ['messageId' => $message->handle],
            ]);
        } else {
            // Acknowledge to discard (move to DLQ if configured)
            $this->ack($message);
        }
    }

    public function purge(string $queue): void
    {
        $this->http->delete($this->topicUrl($queue) . '/truncate');
    }

    public function size(string $queue): int
    {
        $response = $this->http->get(
            "admin/v2/{$this->tenant}/{$this->namespace}/{$queue}/stats"
        );

        $data = json_decode((string) $response->getBody(), true);

        return (int) ($data['msgInCounter'] ?? 0);
    }

    public function disconnect(): void
    {
        // HTTP client; nothing to close
    }

    // ---------------------------------------------------------------

    private function topicUrl(string $topic): string
    {
        return "admin/v2/{$this->tenant}/{$this->namespace}/{$topic}";
    }

    private function consumerUrl(string $topic): string
    {
        return "topics/{$this->tenant}/{$this->namespace}/{$topic}/" .
               rawurlencode($this->subscription) . '/' . rawurlencode($this->subType);
    }
}
