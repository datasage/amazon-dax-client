<?php

declare(strict_types=1);

namespace Dax\Tests\Unit\Cache;

use Dax\Cache\KeySchemaCache;
use Dax\Cache\AttributeListCache;
use PHPUnit\Framework\TestCase;
use Psr\SimpleCache\CacheInterface;

/**
 * PSR-16 compliance tests for cache implementations
 */
class Psr16ComplianceTest extends TestCase
{
    public function testKeySchemaCache_ImplementsPsr16Interface(): void
    {
        $cache = new KeySchemaCache();
        $this->assertInstanceOf(CacheInterface::class, $cache);
    }

    public function testAttributeListCache_ImplementsPsr16Interface(): void
    {
        $cache = new AttributeListCache();
        $this->assertInstanceOf(CacheInterface::class, $cache);
    }

    public function testKeySchemaCache_Psr16Methods(): void
    {
        $cache = new KeySchemaCache();
        
        // Test set and get
        $this->assertTrue($cache->set('test_key', ['test' => 'data']));
        $this->assertEquals(['test' => 'data'], $cache->get('test_key'));
        $this->assertEquals('default', $cache->get('nonexistent', 'default'));
        
        // Test has
        $this->assertTrue($cache->has('test_key'));
        $this->assertFalse($cache->has('nonexistent'));
        
        // Test delete
        $this->assertTrue($cache->delete('test_key'));
        $this->assertFalse($cache->has('test_key'));
        $this->assertFalse($cache->delete('nonexistent'));
        
        // Test clear
        $cache->set('key1', 'value1');
        $cache->set('key2', 'value2');
        $this->assertTrue($cache->clear());
        $this->assertFalse($cache->has('key1'));
        $this->assertFalse($cache->has('key2'));
    }

    public function testAttributeListCache_Psr16Methods(): void
    {
        $cache = new AttributeListCache();
        
        // Test set and get
        $this->assertTrue($cache->set('test_key', ['attributes' => ['id', 'name']]));
        $this->assertEquals(['attributes' => ['id', 'name']], $cache->get('test_key'));
        $this->assertEquals('default', $cache->get('nonexistent', 'default'));
        
        // Test has
        $this->assertTrue($cache->has('test_key'));
        $this->assertFalse($cache->has('nonexistent'));
        
        // Test delete
        $this->assertTrue($cache->delete('test_key'));
        $this->assertFalse($cache->has('test_key'));
        $this->assertFalse($cache->delete('nonexistent'));
        
        // Test clear
        $cache->set('key1', 'value1');
        $cache->set('key2', 'value2');
        $this->assertTrue($cache->clear());
        $this->assertFalse($cache->has('key1'));
        $this->assertFalse($cache->has('key2'));
    }

    public function testMultipleOperations(): void
    {
        $cache = new KeySchemaCache();
        
        // Test setMultiple
        $values = ['key1' => 'value1', 'key2' => 'value2', 'key3' => 'value3'];
        $this->assertTrue($cache->setMultiple($values));
        
        // Test getMultiple
        $result = $cache->getMultiple(['key1', 'key2', 'nonexistent'], 'default');
        $expected = ['key1' => 'value1', 'key2' => 'value2', 'nonexistent' => 'default'];
        $this->assertEquals($expected, iterator_to_array($result));
        
        // Test deleteMultiple
        $this->assertTrue($cache->deleteMultiple(['key1', 'key3']));
        $this->assertFalse($cache->has('key1'));
        $this->assertTrue($cache->has('key2'));
        $this->assertFalse($cache->has('key3'));
    }

    public function testInvalidKeyValidation(): void
    {
        $cache = new KeySchemaCache();
        
        $this->expectException(\Psr\SimpleCache\InvalidArgumentException::class);
        $cache->get('');
    }

    public function testInvalidKeyWithReservedCharacters(): void
    {
        $cache = new KeySchemaCache();
        
        $this->expectException(\Psr\SimpleCache\InvalidArgumentException::class);
        $cache->set('key{with}reserved', 'value');
    }
}
