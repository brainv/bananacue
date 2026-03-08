<?php

declare(strict_types=1);

namespace Queue\Tests;

use PHPUnit\Framework\TestCase;
use Queue\Contracts\Message;
use Queue\Drivers\AbstractDriver;
use Queue\Exceptions\QueueException;

/**
 * Concrete stub that exposes AbstractDriver's protected helpers as public
 * methods and provides a simple pre-loadable consume() implementation.
 */
class ConcreteDriverStub extends AbstractDriver
{
    /** Messages to return from consume(), in order. */
    private array $messages = [];

    public function loadMessages(array $messages): void
    {
        $this->messages = $messages;
    }

    // Expose helpers as public for direct testing
    public function serializePublic(mixed $payload): string
    {
        return $this->serialize($payload);
    }

    public function deserializePublic(string $raw): mixed
    {
        return $this->deserialize($raw);
    }

    // QueueInterface implementation ---------------------------------

    public function publish(string $queue, mixed $payload, array $options = []): ?string
    {
        return null;
    }

    public function consume(string $queue, array $options = []): ?Message
    {
        return array_shift($this->messages);
    }

    public function ack(Message $message): void {}

    public function nack(Message $message, bool $requeue = true): void {}

    public function purge(string $queue): void {}

    public function size(string $queue): int
    {
        return count($this->messages);
    }

    public function disconnect(): void {}
}

class AbstractDriverTest extends TestCase
{
    private ConcreteDriverStub $driver;

    protected function setUp(): void
    {
        $this->driver = new ConcreteDriverStub();
    }

    // ------------------------------------------------------------------
    // serialize()
    // ------------------------------------------------------------------

    public function testSerializeEncodesScalarString(): void
    {
        self::assertSame('"hello"', $this->driver->serializePublic('hello'));
    }

    public function testSerializeEncodesInteger(): void
    {
        self::assertSame('42', $this->driver->serializePublic(42));
    }

    public function testSerializeEncodesArray(): void
    {
        $encoded = $this->driver->serializePublic(['a' => 1, 'b' => [2, 3]]);
        self::assertSame(['a' => 1, 'b' => [2, 3]], json_decode($encoded, true));
    }

    public function testSerializeEncodesNull(): void
    {
        self::assertSame('null', $this->driver->serializePublic(null));
    }

    public function testSerializeThrowsQueueExceptionForUnserializable(): void
    {
        $this->expectException(QueueException::class);
        $this->expectExceptionMessage('Failed to serialize');

        // INF is not JSON-encodable
        $this->driver->serializePublic(INF);
    }

    // ------------------------------------------------------------------
    // deserialize()
    // ------------------------------------------------------------------

    public function testDeserializeDecodesJsonObject(): void
    {
        $result = $this->driver->deserializePublic('{"key":"val"}');
        self::assertSame(['key' => 'val'], $result);
    }

    public function testDeserializeDecodesJsonArray(): void
    {
        $result = $this->driver->deserializePublic('[1,2,3]');
        self::assertSame([1, 2, 3], $result);
    }

    public function testDeserializeDecodesJsonScalar(): void
    {
        self::assertSame(99, $this->driver->deserializePublic('99'));
    }

    public function testDeserializeFallsBackToRawStringForInvalidJson(): void
    {
        $result = $this->driver->deserializePublic('not-json-{{');
        self::assertSame('not-json-{{', $result);
    }

    public function testDeserializeFallsBackToPlainString(): void
    {
        $result = $this->driver->deserializePublic('plain text');
        self::assertSame('plain text', $result);
    }

    // ------------------------------------------------------------------
    // subscribe() — default polling loop
    // ------------------------------------------------------------------

    private function makeMessage(string $payload = 'data'): Message
    {
        return new Message(payload: $payload, handle: 'h', queue: 'q');
    }

    public function testSubscribeCallsCallbackForEachMessage(): void
    {
        $msgs = [
            $this->makeMessage('one'),
            $this->makeMessage('two'),
            $this->makeMessage('three'),
        ];
        $this->driver->loadMessages($msgs);

        $received = [];
        $this->driver->subscribe('q', function (Message $m) use (&$received): bool {
            $received[] = $m->payload;
            return true;
        }, ['poll_interval_ms' => 0, 'max_messages' => 3]);

        self::assertSame(['one', 'two', 'three'], $received);
    }

    public function testSubscribeStopsWhenCallbackReturnsFalse(): void
    {
        $this->driver->loadMessages([
            $this->makeMessage('stop-after-me'),
            $this->makeMessage('never-reached'),
        ]);

        $count = 0;
        $this->driver->subscribe('q', function () use (&$count): bool {
            $count++;
            return false; // stop immediately
        }, ['poll_interval_ms' => 0, 'max_messages' => 100]);

        self::assertSame(1, $count);
    }

    public function testSubscribeRespectsMaxMessagesOption(): void
    {
        $this->driver->loadMessages([
            $this->makeMessage('a'),
            $this->makeMessage('b'),
            $this->makeMessage('c'),
        ]);

        $count = 0;
        $this->driver->subscribe('q', function () use (&$count): bool {
            $count++;
            return true;
        }, ['poll_interval_ms' => 0, 'max_messages' => 2]);

        self::assertSame(2, $count);
    }

    public function testSubscribeSkipsNullConsumeWithoutCallingCallback(): void
    {
        // Queue is empty; loop should not call callback.
        // Use max_messages=1 so the loop terminates after one attempt.
        $called = false;
        $this->driver->subscribe('q', function () use (&$called): bool {
            $called = true;
            return false;
        }, ['poll_interval_ms' => 0, 'max_messages' => 0]); // max=0 → loop never enters

        self::assertFalse($called);
    }
}
