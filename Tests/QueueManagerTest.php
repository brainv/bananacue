<?php

declare(strict_types=1);

namespace Queue\Tests;

use PHPUnit\Framework\TestCase;
use Queue\Contracts\Message;
use Queue\Contracts\QueueInterface;
use Queue\Drivers\InMemoryDriver;
use Queue\Exceptions\QueueException;
use Queue\QueueManager;

class QueueManagerTest extends TestCase
{
    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private function makeMessage(): Message
    {
        return new Message(payload: 'p', handle: 'h', queue: 'q');
    }

    /** Build a manager whose default connection resolves to InMemoryDriver. */
    private function memoryManager(): QueueManager
    {
        return new QueueManager([
            'default'     => 'mem',
            'connections' => [
                'mem' => ['driver' => 'memory'],
            ],
        ]);
    }

    /** Build a manager with a mock default connection injected via extend(). */
    private function mockManager(): array
    {
        $mock    = $this->createMock(QueueInterface::class);
        $manager = new QueueManager([
            'default'     => 'fake',
            'connections' => [
                'fake' => ['driver' => 'fake'],
            ],
        ]);
        $manager->extend('fake', fn(array $cfg) => $mock);

        return [$manager, $mock];
    }

    // ------------------------------------------------------------------
    // Default configuration
    // ------------------------------------------------------------------

    public function testDefaultConnectionIsMemoryWhenNoConfigGiven(): void
    {
        $manager = new QueueManager();
        $conn    = $manager->connection();

        self::assertInstanceOf(InMemoryDriver::class, $conn);
    }

    // ------------------------------------------------------------------
    // connection()
    // ------------------------------------------------------------------

    public function testConnectionWithoutArgReturnsDefaultConnection(): void
    {
        $manager = $this->memoryManager();
        $a = $manager->connection();
        $b = $manager->connection(null);

        self::assertSame($a, $b);
    }

    public function testConnectionWithNameReturnsNamedDriver(): void
    {
        $manager = new QueueManager([
            'default' => 'mem',
            'connections' => [
                'mem'  => ['driver' => 'memory'],
                'mem2' => ['driver' => 'array'],
            ],
        ]);

        $a = $manager->connection('mem');
        $b = $manager->connection('mem2');

        self::assertInstanceOf(InMemoryDriver::class, $a);
        self::assertInstanceOf(InMemoryDriver::class, $b);
        self::assertNotSame($a, $b);
    }

    public function testConnectionCachesResolvedInstance(): void
    {
        $manager = $this->memoryManager();
        $first   = $manager->connection('mem');
        $second  = $manager->connection('mem');

        self::assertSame($first, $second);
    }

    public function testConnectionThrowsForUndefinedConnectionName(): void
    {
        $this->expectException(QueueException::class);
        $this->expectExceptionMessage('undefined-conn');

        $manager = $this->memoryManager();
        $manager->connection('undefined-conn');
    }

    public function testConnectionThrowsForUnknownDriver(): void
    {
        $this->expectException(QueueException::class);

        $manager = new QueueManager([
            'default'     => 'x',
            'connections' => ['x' => ['driver' => 'totally-unknown']],
        ]);
        $manager->connection('x');
    }

    // ------------------------------------------------------------------
    // extend()
    // ------------------------------------------------------------------

    public function testExtendRegistersCustomDriverFactory(): void
    {
        $custom  = $this->createMock(QueueInterface::class);
        $manager = new QueueManager([
            'default'     => 'my-driver',
            'connections' => ['my-driver' => ['driver' => 'my-driver', 'opt' => 'val']],
        ]);
        $manager->extend('my-driver', fn(array $cfg) => $custom);

        self::assertSame($custom, $manager->connection('my-driver'));
    }

    public function testExtendFactoryReceivesConnectionConfig(): void
    {
        $capturedConfig = [];
        $manager = new QueueManager([
            'default'     => 'd',
            'connections' => ['d' => ['driver' => 'd', 'key' => 'value']],
        ]);
        $manager->extend('d', function (array $cfg) use (&$capturedConfig): QueueInterface {
            $capturedConfig = $cfg;
            return $this->createMock(QueueInterface::class);
        });

        $manager->connection('d');

        self::assertSame(['driver' => 'd', 'key' => 'value'], $capturedConfig);
    }

    public function testExtendOverridesBuiltInDriver(): void
    {
        $custom  = $this->createMock(QueueInterface::class);
        $manager = new QueueManager([
            'default'     => 'mem',
            'connections' => ['mem' => ['driver' => 'memory']],
        ]);
        $manager->extend('memory', fn() => $custom);

        self::assertSame($custom, $manager->connection('mem'));
    }

    // ------------------------------------------------------------------
    // forgetConnection()
    // ------------------------------------------------------------------

    public function testForgetConnectionCallsDisconnectOnDriver(): void
    {
        $mock = $this->createMock(QueueInterface::class);
        $mock->expects(self::once())->method('disconnect');

        $manager = new QueueManager([
            'default'     => 'd',
            'connections' => ['d' => ['driver' => 'd']],
        ]);
        $manager->extend('d', fn() => $mock);
        $manager->connection('d'); // resolve/cache

        $manager->forgetConnection('d');
    }

    public function testForgetConnectionRemovesFromCache(): void
    {
        $callCount = 0;
        $manager   = new QueueManager([
            'default'     => 'd',
            'connections' => ['d' => ['driver' => 'd']],
        ]);
        $manager->extend('d', function () use (&$callCount): QueueInterface {
            $callCount++;
            return $this->createMock(QueueInterface::class);
        });

        $manager->connection('d'); // resolves once
        $manager->forgetConnection('d');
        $manager->connection('d'); // should resolve again

        self::assertSame(2, $callCount);
    }

    public function testForgetConnectionOnNonExistentIsNoOp(): void
    {
        // Should not throw
        $manager = $this->memoryManager();
        $manager->forgetConnection('does-not-exist');
        self::assertTrue(true);
    }

    // ------------------------------------------------------------------
    // disconnect() — disconnects all resolved connections
    // ------------------------------------------------------------------

    public function testDisconnectCallsDisconnectOnAllResolvedConnections(): void
    {
        $mockA = $this->createMock(QueueInterface::class);
        $mockA->expects(self::once())->method('disconnect');

        $mockB = $this->createMock(QueueInterface::class);
        $mockB->expects(self::once())->method('disconnect');

        $manager = new QueueManager([
            'default'     => 'a',
            'connections' => [
                'a' => ['driver' => 'a'],
                'b' => ['driver' => 'b'],
            ],
        ]);
        $manager->extend('a', fn() => $mockA);
        $manager->extend('b', fn() => $mockB);

        $manager->connection('a');
        $manager->connection('b');
        $manager->disconnect();
    }

    // ------------------------------------------------------------------
    // QueueInterface proxy methods
    // ------------------------------------------------------------------

    public function testPublishDelegatesToDefaultConnection(): void
    {
        [$manager, $mock] = $this->mockManager();
        $mock->expects(self::once())
             ->method('publish')
             ->with('orders', ['id' => 1], [])
             ->willReturn('msg-id');

        $result = $manager->publish('orders', ['id' => 1]);
        self::assertSame('msg-id', $result);
    }

    public function testConsumeDelegatesToDefaultConnection(): void
    {
        [$manager, $mock] = $this->mockManager();
        $msg = $this->makeMessage();
        $mock->expects(self::once())
             ->method('consume')
             ->with('orders', [])
             ->willReturn($msg);

        self::assertSame($msg, $manager->consume('orders'));
    }

    public function testSubscribeDelegatesToDefaultConnection(): void
    {
        [$manager, $mock] = $this->mockManager();
        $cb = fn(Message $m): bool => true;
        $mock->expects(self::once())
             ->method('subscribe')
             ->with('orders', $cb, []);

        $manager->subscribe('orders', $cb);
    }

    public function testAckDelegatesToDefaultConnection(): void
    {
        [$manager, $mock] = $this->mockManager();
        $msg = $this->makeMessage();
        $mock->expects(self::once())
             ->method('ack')
             ->with($msg);

        $manager->ack($msg);
    }

    public function testNackDelegatesToDefaultConnection(): void
    {
        [$manager, $mock] = $this->mockManager();
        $msg = $this->makeMessage();
        $mock->expects(self::once())
             ->method('nack')
             ->with($msg, false);

        $manager->nack($msg, false);
    }

    public function testPurgeDelegatesToDefaultConnection(): void
    {
        [$manager, $mock] = $this->mockManager();
        $mock->expects(self::once())
             ->method('purge')
             ->with('orders');

        $manager->purge('orders');
    }

    public function testSizeDelegatesToDefaultConnection(): void
    {
        [$manager, $mock] = $this->mockManager();
        $mock->expects(self::once())
             ->method('size')
             ->with('orders')
             ->willReturn(7);

        self::assertSame(7, $manager->size('orders'));
    }
}
