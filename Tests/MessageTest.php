<?php

declare(strict_types=1);

namespace Queue\Tests;

use PHPUnit\Framework\TestCase;
use Queue\Contracts\Message;

class MessageTest extends TestCase
{
    // ------------------------------------------------------------------
    // Constructor / property assignment
    // ------------------------------------------------------------------

    public function testConstructorSetsAllProperties(): void
    {
        $msg = new Message(
            payload:  ['order_id' => 42],
            handle:   'handle-abc',
            queue:    'orders',
            id:       'msg-001',
            meta:     ['priority' => 5],
            attempts: 3,
        );

        self::assertSame(['order_id' => 42], $msg->payload);
        self::assertSame('handle-abc', $msg->handle);
        self::assertSame('orders', $msg->queue);
        self::assertSame('msg-001', $msg->id);
        self::assertSame(['priority' => 5], $msg->meta);
        self::assertSame(3, $msg->attempts);
    }

    public function testDefaultIdIsNull(): void
    {
        $msg = new Message(payload: 'hello', handle: 'h', queue: 'q');
        self::assertNull($msg->id);
    }

    public function testDefaultMetaIsEmptyArray(): void
    {
        $msg = new Message(payload: 'hello', handle: 'h', queue: 'q');
        self::assertSame([], $msg->meta);
    }

    public function testDefaultAttemptsIsZero(): void
    {
        $msg = new Message(payload: 'hello', handle: 'h', queue: 'q');
        self::assertSame(0, $msg->attempts);
    }

    public function testPayloadCanBeNull(): void
    {
        $msg = new Message(payload: null, handle: 'h', queue: 'q');
        self::assertNull($msg->payload);
    }

    public function testPayloadCanBeScalar(): void
    {
        $msg = new Message(payload: 'plain text', handle: 'h', queue: 'q');
        self::assertSame('plain text', $msg->payload);
    }

    // ------------------------------------------------------------------
    // withMeta()
    // ------------------------------------------------------------------

    public function testWithMetaReturnsNewInstance(): void
    {
        $original = new Message(payload: 'x', handle: 'h', queue: 'q');
        $copy     = $original->withMeta(['key' => 'val']);

        self::assertNotSame($original, $copy);
    }

    public function testWithMetaMergesNewKeys(): void
    {
        $original = new Message(payload: 'x', handle: 'h', queue: 'q', meta: ['a' => 1]);
        $copy     = $original->withMeta(['b' => 2]);

        self::assertSame(['a' => 1, 'b' => 2], $copy->meta);
    }

    public function testWithMetaOverridesExistingKeys(): void
    {
        $original = new Message(payload: 'x', handle: 'h', queue: 'q', meta: ['a' => 1]);
        $copy     = $original->withMeta(['a' => 99]);

        self::assertSame(['a' => 99], $copy->meta);
    }

    public function testWithMetaDoesNotMutateOriginal(): void
    {
        $original = new Message(payload: 'x', handle: 'h', queue: 'q', meta: ['a' => 1]);
        $original->withMeta(['b' => 2]);

        self::assertSame(['a' => 1], $original->meta);
    }

    public function testWithMetaPreservesOtherProperties(): void
    {
        $original = new Message(
            payload:  ['data' => true],
            handle:   'handle-xyz',
            queue:    'events',
            id:       'id-999',
            meta:     [],
            attempts: 2,
        );

        $copy = $original->withMeta(['region' => 'us-east-1']);

        self::assertSame($original->payload,  $copy->payload);
        self::assertSame($original->handle,   $copy->handle);
        self::assertSame($original->queue,    $copy->queue);
        self::assertSame($original->id,       $copy->id);
        self::assertSame($original->attempts, $copy->attempts);
    }

    public function testWithMetaOnEmptyMetaAddsKey(): void
    {
        $msg  = new Message(payload: 'x', handle: 'h', queue: 'q');
        $copy = $msg->withMeta(['source' => 'sqs']);

        self::assertSame(['source' => 'sqs'], $copy->meta);
    }
}
