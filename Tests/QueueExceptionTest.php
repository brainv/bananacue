<?php

declare(strict_types=1);

namespace Queue\Tests;

use PHPUnit\Framework\TestCase;
use Queue\Exceptions\QueueException;

class QueueExceptionTest extends TestCase
{
    public function testIsInstanceOfRuntimeException(): void
    {
        $e = new QueueException('oops');
        self::assertInstanceOf(\RuntimeException::class, $e);
    }

    public function testPreservesMessage(): void
    {
        $e = new QueueException('Connection refused');
        self::assertSame('Connection refused', $e->getMessage());
    }

    public function testPreservesCode(): void
    {
        $e = new QueueException('err', 42);
        self::assertSame(42, $e->getCode());
    }

    public function testPreservesPreviousException(): void
    {
        $previous = new \RuntimeException('root cause');
        $e        = new QueueException('wrapped', 0, $previous);

        self::assertSame($previous, $e->getPrevious());
    }

    public function testCanBeThrownAndCaught(): void
    {
        $this->expectException(QueueException::class);
        $this->expectExceptionMessage('boom');

        throw new QueueException('boom');
    }

    public function testCanBeCaughtAsRuntimeException(): void
    {
        $caught = null;

        try {
            throw new QueueException('caught as parent');
        } catch (\RuntimeException $e) {
            $caught = $e;
        }

        self::assertInstanceOf(QueueException::class, $caught);
        self::assertSame('caught as parent', $caught->getMessage());
    }
}
