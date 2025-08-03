<?php

declare(strict_types=1);

namespace Dax\Tests\Unit\Cache;

use Dax\Cache\KeySchemaCache;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for KeySchemaCache
 */
class KeySchemaCacheTest extends TestCase
{
    private KeySchemaCache $cache;

    protected function setUp(): void
    {
        $this->cache = new KeySchemaCache(3, 1000); // Small cache with 1 second TTL for testing
    }

    public function testPutAndGet(): void
    {
        $tableName = 'TestTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
            'RangeKeyElement' => ['AttributeName' => 'sort', 'AttributeType' => 'N']
        ];

        $this->cache->put($tableName, $keySchema);
        $retrieved = $this->cache->get($tableName);

        $this->assertEquals($keySchema, $retrieved);
    }

    public function testGetNonExistentTable(): void
    {
        $result = $this->cache->get('NonExistentTable');
        $this->assertNull($result);
    }

    public function testHas(): void
    {
        $tableName = 'TestTable';
        $keySchema = ['HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S']];

        $this->assertFalse($this->cache->has($tableName));
        
        $this->cache->put($tableName, $keySchema);
        $this->assertTrue($this->cache->has($tableName));
    }

    public function testRemove(): void
    {
        $tableName = 'TestTable';
        $keySchema = ['HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S']];

        $this->cache->put($tableName, $keySchema);
        $this->assertTrue($this->cache->has($tableName));

        $this->cache->remove($tableName);
        $this->assertFalse($this->cache->has($tableName));
    }

    public function testClear(): void
    {
        $this->cache->put('Table1', ['HashKeyElement' => ['AttributeName' => 'id1', 'AttributeType' => 'S']]);
        $this->cache->put('Table2', ['HashKeyElement' => ['AttributeName' => 'id2', 'AttributeType' => 'S']]);

        $this->assertTrue($this->cache->has('Table1'));
        $this->assertTrue($this->cache->has('Table2'));

        $this->cache->clear();

        $this->assertFalse($this->cache->has('Table1'));
        $this->assertFalse($this->cache->has('Table2'));
    }

    public function testMaxSizeEviction(): void
    {
        // Cache has max size of 3
        $this->cache->put('Table1', ['HashKeyElement' => ['AttributeName' => 'id1', 'AttributeType' => 'S']]);
        $this->cache->put('Table2', ['HashKeyElement' => ['AttributeName' => 'id2', 'AttributeType' => 'S']]);
        $this->cache->put('Table3', ['HashKeyElement' => ['AttributeName' => 'id3', 'AttributeType' => 'S']]);

        $this->assertTrue($this->cache->has('Table1'));
        $this->assertTrue($this->cache->has('Table2'));
        $this->assertTrue($this->cache->has('Table3'));

        // Adding a 4th item should evict the oldest (Table1)
        $this->cache->put('Table4', ['HashKeyElement' => ['AttributeName' => 'id4', 'AttributeType' => 'S']]);

        $this->assertFalse($this->cache->has('Table1')); // Evicted
        $this->assertTrue($this->cache->has('Table2'));
        $this->assertTrue($this->cache->has('Table3'));
        $this->assertTrue($this->cache->has('Table4'));
    }

    public function testTtlExpiration(): void
    {
        $cache = new KeySchemaCache(10, 100); // 100ms TTL
        $tableName = 'TestTable';
        $keySchema = ['HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S']];

        $cache->put($tableName, $keySchema);
        $this->assertTrue($cache->has($tableName));

        // Wait for TTL to expire
        usleep(150000); // 150ms

        $this->assertFalse($cache->has($tableName));
        $this->assertNull($cache->get($tableName));
    }

    public function testGetTableNames(): void
    {
        $this->cache->put('Table1', ['HashKeyElement' => ['AttributeName' => 'id1', 'AttributeType' => 'S']]);
        $this->cache->put('Table2', ['HashKeyElement' => ['AttributeName' => 'id2', 'AttributeType' => 'S']]);

        $tableNames = $this->cache->getTableNames();
        $this->assertCount(2, $tableNames);
        $this->assertContains('Table1', $tableNames);
        $this->assertContains('Table2', $tableNames);
    }

    public function testGetStats(): void
    {
        $this->cache->put('Table1', ['HashKeyElement' => ['AttributeName' => 'id1', 'AttributeType' => 'S']]);
        $this->cache->put('Table2', ['HashKeyElement' => ['AttributeName' => 'id2', 'AttributeType' => 'S']]);

        $stats = $this->cache->getStats();

        $this->assertIsArray($stats);
        $this->assertArrayHasKey('size', $stats);
        $this->assertArrayHasKey('max_size', $stats);
        $this->assertArrayHasKey('ttl_millis', $stats);
        $this->assertArrayHasKey('expired_count', $stats);

        $this->assertEquals(2, $stats['size']);
        $this->assertEquals(3, $stats['max_size']);
        $this->assertEquals(1000, $stats['ttl_millis']);
    }

    public function testUpdateExistingEntry(): void
    {
        $tableName = 'TestTable';
        $keySchema1 = ['HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S']];
        $keySchema2 = ['HashKeyElement' => ['AttributeName' => 'pk', 'AttributeType' => 'S']];

        $this->cache->put($tableName, $keySchema1);
        $this->assertEquals($keySchema1, $this->cache->get($tableName));

        // Update with new schema
        $this->cache->put($tableName, $keySchema2);
        $this->assertEquals($keySchema2, $this->cache->get($tableName));

        // Should still have only one entry
        $this->assertCount(1, $this->cache->getTableNames());
    }

    public function testEmptyCache(): void
    {
        $stats = $this->cache->getStats();
        $this->assertEquals(0, $stats['size']);
        $this->assertEmpty($this->cache->getTableNames());
    }
}
