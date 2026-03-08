<?php

declare(strict_types=1);

namespace Queue\Tests;

use PHPUnit\Framework\TestCase;
use Queue\Contracts\Message;
use Queue\Drivers\InMemoryDriver;

class InMemoryDriverTest extends TestCase
{
    private InMemoryDriver $driver;

    protected function setUp(): void
    {
        $this->driver = new InMemoryDriver();
    }

    // ------------------------------------------------------------------
    // publish()
    // ------------------------------------------------------------------

    public function testPublishReturnsStringId(): void
    {
        $id = $this->driver->publish('jobs', ['task' => 'send-email']);
        self::assertIsString($id);
        self::assertNotEmpty($id);
    }

    public function testPublishReturnsProvidedId(): void
    {
        $id = $this->driver->publish('jobs', 'payload', ['id' => 'custom-id-42']);
        self::assertSame('custom-id-42', $id);
    }

    public function testPublishWithMetaOptionStoresMetaOnMessage(): void
    {
        $this->driver->publish('jobs', 'payload', ['meta' => ['region' => 'eu-west-1']]);
        $msg = $this->driver->consume('jobs');

        self::assertSame(['region' => 'eu-west-1'], $msg->meta);
    }

    public function testPublishIncreasesSize(): void
    {
        self::assertSame(0, $this->driver->size('jobs'));

        $this->driver->publish('jobs', 'a');
        self::assertSame(1, $this->driver->size('jobs'));

        $this->driver->publish('jobs', 'b');
        self::assertSame(2, $this->driver->size('jobs'));
    }

    // ------------------------------------------------------------------
    // consume()
    // ------------------------------------------------------------------

    public function testConsumeOnEmptyQueueReturnsNull(): void
    {
        self::assertNull($this->driver->consume('empty'));
    }

    public function testConsumeReturnsMessageWithCorrectPayload(): void
    {
        $this->driver->publish('jobs', ['key' => 'value']);
        $msg = $this->driver->consume('jobs');

        self::assertInstanceOf(Message::class, $msg);
        self::assertSame(['key' => 'value'], $msg->payload);
    }

    public function testConsumeReturnsMessageWithCorrectQueue(): void
    {
        $this->driver->publish('events', 'event-data');
        $msg = $this->driver->consume('events');

        self::assertSame('events', $msg->queue);
    }

    public function testConsumeReturnsMessageWithCorrectId(): void
    {
        $id = $this->driver->publish('jobs', 'data', ['id' => 'known-id']);
        $msg = $this->driver->consume('jobs');

        self::assertSame('known-id', $msg->id);
    }

    public function testConsumeFifoOrder(): void
    {
        $this->driver->publish('jobs', 'first');
        $this->driver->publish('jobs', 'second');
        $this->driver->publish('jobs', 'third');

        self::assertSame('first',  $this->driver->consume('jobs')->payload);
        self::assertSame('second', $this->driver->consume('jobs')->payload);
        self::assertSame('third',  $this->driver->consume('jobs')->payload);
    }

    public function testConsumeDecrementsSize(): void
    {
        $this->driver->publish('jobs', 'a');
        $this->driver->publish('jobs', 'b');
        $this->driver->consume('jobs');

        self::assertSame(1, $this->driver->size('jobs'));
    }

    public function testConsumeReturnsNullAfterQueueDrained(): void
    {
        $this->driver->publish('jobs', 'only');
        $this->driver->consume('jobs');

        self::assertNull($this->driver->consume('jobs'));
    }

    // ------------------------------------------------------------------
    // ack()
    // ------------------------------------------------------------------

    public function testAckIsNoOpAndDoesNotThrow(): void
    {
        $this->driver->publish('jobs', 'data');
        $msg = $this->driver->consume('jobs');

        // Should not throw; queue stays empty (message was already removed on consume)
        $this->driver->ack($msg);
        self::assertSame(0, $this->driver->size('jobs'));
    }

    // ------------------------------------------------------------------
    // nack()
    // ------------------------------------------------------------------

    public function testNackWithRequeuePushesMessageBackToFront(): void
    {
        $this->driver->publish('jobs', 'requeued');
        $this->driver->publish('jobs', 'second');
        $msg = $this->driver->consume('jobs');

        $this->driver->nack($msg, requeue: true);

        $requeued = $this->driver->consume('jobs');
        self::assertSame('requeued', $requeued->payload);
    }

    public function testNackWithRequeueIncreasesSize(): void
    {
        $this->driver->publish('jobs', 'data');
        $msg = $this->driver->consume('jobs');
        self::assertSame(0, $this->driver->size('jobs'));

        $this->driver->nack($msg, requeue: true);
        self::assertSame(1, $this->driver->size('jobs'));
    }

    public function testNackWithoutRequeuDiscardsMessage(): void
    {
        $this->driver->publish('jobs', 'discard-me');
        $msg = $this->driver->consume('jobs');

        $this->driver->nack($msg, requeue: false);

        self::assertSame(0, $this->driver->size('jobs'));
        self::assertNull($this->driver->consume('jobs'));
    }

    public function testNackDefaultRequeueIsTrue(): void
    {
        $this->driver->publish('jobs', 'default-requeue');
        $msg = $this->driver->consume('jobs');

        $this->driver->nack($msg); // default requeue=true

        self::assertSame(1, $this->driver->size('jobs'));
    }

    // ------------------------------------------------------------------
    // size()
    // ------------------------------------------------------------------

    public function testSizeOnUninitializedQueueIsZero(): void
    {
        self::assertSame(0, $this->driver->size('never-touched'));
    }

    public function testSizeReflectsMultiplePublishes(): void
    {
        $this->driver->publish('q', 1);
        $this->driver->publish('q', 2);
        $this->driver->publish('q', 3);

        self::assertSame(3, $this->driver->size('q'));
    }

    // ------------------------------------------------------------------
    // purge()
    // ------------------------------------------------------------------

    public function testPurgeClearsAllMessages(): void
    {
        $this->driver->publish('jobs', 'a');
        $this->driver->publish('jobs', 'b');
        $this->driver->purge('jobs');

        self::assertSame(0, $this->driver->size('jobs'));
        self::assertNull($this->driver->consume('jobs'));
    }

    public function testPurgeOnEmptyQueueIsNoOp(): void
    {
        $this->driver->purge('empty');
        self::assertSame(0, $this->driver->size('empty'));
    }

    // ------------------------------------------------------------------
    // all() — inspection helper
    // ------------------------------------------------------------------

    public function testAllReturnsAllPendingMessages(): void
    {
        $this->driver->publish('jobs', 'x', ['id' => 'id-1']);
        $this->driver->publish('jobs', 'y', ['id' => 'id-2']);

        $all = $this->driver->all('jobs');

        self::assertCount(2, $all);
        self::assertSame('id-1', $all[0]['id']);
        self::assertSame('id-2', $all[1]['id']);
    }

    public function testAllDoesNotConsumeMessages(): void
    {
        $this->driver->publish('jobs', 'data');
        $this->driver->all('jobs');

        self::assertSame(1, $this->driver->size('jobs'));
    }

    public function testAllOnEmptyQueueReturnsEmptyArray(): void
    {
        self::assertSame([], $this->driver->all('nothing'));
    }

    // ------------------------------------------------------------------
    // disconnect()
    // ------------------------------------------------------------------

    public function testDisconnectClearsAllQueues(): void
    {
        $this->driver->publish('a', 1);
        $this->driver->publish('b', 2);
        $this->driver->disconnect();

        self::assertSame(0, $this->driver->size('a'));
        self::assertSame(0, $this->driver->size('b'));
    }

    // ------------------------------------------------------------------
    // Multiple queues are independent
    // ------------------------------------------------------------------

    public function testMultipleQueuesAreIsolated(): void
    {
        $this->driver->publish('alpha', 'msg-alpha');
        $this->driver->publish('beta',  'msg-beta');

        self::assertSame(1, $this->driver->size('alpha'));
        self::assertSame(1, $this->driver->size('beta'));

        $msgAlpha = $this->driver->consume('alpha');
        self::assertSame('msg-alpha', $msgAlpha->payload);
        self::assertNull($this->driver->consume('alpha'));

        // beta untouched
        self::assertSame(1, $this->driver->size('beta'));
    }
}
