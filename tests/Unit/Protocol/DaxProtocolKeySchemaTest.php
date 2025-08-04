<?php

declare(strict_types=1);

namespace Dax\Tests\Unit\Protocol;

use Dax\Auth\DaxAuthenticator;
use Dax\Cache\KeySchemaCache;
use Dax\Exception\DaxException;
use Dax\Protocol\DaxProtocol;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Unit tests for DaxProtocol key schema functionality
 */
class DaxProtocolKeySchemaTest extends TestCase
{
    private DaxProtocol $protocol;
    private KeySchemaCache $keySchemaCache;

    protected function setUp(): void
    {
        $this->keySchemaCache = new KeySchemaCache(100, 60000);

        $config = [
            'region' => 'us-east-1',
            'key_schema_cache' => $this->keySchemaCache,
            'debug_logging' => false,
        ];

        $this->protocol = new DaxProtocol($config, new NullLogger(), null);
    }

    public function testCacheKeySchema(): void
    {
        $tableName = 'TestTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
            'RangeKeyElement' => ['AttributeName' => 'sort', 'AttributeType' => 'N'],
        ];

        $this->protocol->cacheKeySchema($tableName, $keySchema);
        $cached = $this->protocol->getCachedKeySchema($tableName);

        $this->assertEquals($keySchema, $cached);
    }

    public function testGetCachedKeySchemaReturnsNull(): void
    {
        $result = $this->protocol->getCachedKeySchema('NonExistentTable');
        $this->assertNull($result);
    }

    public function testValidateKeyWithHashKeyOnly(): void
    {
        $tableName = 'HashOnlyTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
        ];

        $this->protocol->cacheKeySchema($tableName, $keySchema);

        // This should not throw an exception
        $request = [
            'TableName' => $tableName,
            'Key' => ['id' => ['S' => 'test-id']],
        ];

        // Use reflection to call the private method for testing
        $reflection = new \ReflectionClass($this->protocol);
        $method = $reflection->getMethod('prepareSingleItemRequest');
        $method->setAccessible(true);

        $result = $method->invoke($this->protocol, $request);
        $this->assertArrayHasKey('Key', $result);
    }

    public function testValidateKeyWithHashAndRangeKey(): void
    {
        $tableName = 'HashRangeTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
            'RangeKeyElement' => ['AttributeName' => 'sort', 'AttributeType' => 'N'],
        ];

        $this->protocol->cacheKeySchema($tableName, $keySchema);

        // This should not throw an exception
        $request = [
            'TableName' => $tableName,
            'Key' => [
                'id' => ['S' => 'test-id'],
                'sort' => ['N' => '123'],
            ],
        ];

        // Use reflection to call the private method for testing
        $reflection = new \ReflectionClass($this->protocol);
        $method = $reflection->getMethod('prepareSingleItemRequest');
        $method->setAccessible(true);

        $result = $method->invoke($this->protocol, $request);
        $this->assertArrayHasKey('Key', $result);
    }

    public function testValidateKeyMissingHashKey(): void
    {
        $tableName = 'TestTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
        ];

        $this->protocol->cacheKeySchema($tableName, $keySchema);

        $request = [
            'TableName' => $tableName,
            'Key' => ['wrong_key' => ['S' => 'test-value']],
        ];

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage("Missing hash key 'id' for table 'TestTable'");

        // Use reflection to call the private method for testing
        $reflection = new \ReflectionClass($this->protocol);
        $method = $reflection->getMethod('prepareSingleItemRequest');
        $method->setAccessible(true);

        $method->invoke($this->protocol, $request);
    }

    public function testValidateKeyMissingRangeKey(): void
    {
        $tableName = 'TestTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
            'RangeKeyElement' => ['AttributeName' => 'sort', 'AttributeType' => 'N'],
        ];

        $this->protocol->cacheKeySchema($tableName, $keySchema);

        $request = [
            'TableName' => $tableName,
            'Key' => ['id' => ['S' => 'test-id']], // Missing range key
        ];

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage("Missing range key 'sort' for table 'TestTable'");

        // Use reflection to call the private method for testing
        $reflection = new \ReflectionClass($this->protocol);
        $method = $reflection->getMethod('prepareSingleItemRequest');
        $method->setAccessible(true);

        $method->invoke($this->protocol, $request);
    }

    public function testValidateKeyExtraAttributes(): void
    {
        $tableName = 'TestTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
        ];

        $this->protocol->cacheKeySchema($tableName, $keySchema);

        $request = [
            'TableName' => $tableName,
            'Key' => [
                'id' => ['S' => 'test-id'],
                'extra_attr' => ['S' => 'not-allowed'], // Extra attribute
            ],
        ];

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage("Invalid key attribute 'extra_attr' for table 'TestTable'. Only key attributes are allowed.");

        // Use reflection to call the private method for testing
        $reflection = new \ReflectionClass($this->protocol);
        $method = $reflection->getMethod('prepareSingleItemRequest');
        $method->setAccessible(true);

        $method->invoke($this->protocol, $request);
    }

    public function testPutItemKeyValidation(): void
    {
        $tableName = 'TestTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
            'RangeKeyElement' => ['AttributeName' => 'sort', 'AttributeType' => 'N'],
        ];

        $this->protocol->cacheKeySchema($tableName, $keySchema);

        $request = [
            'TableName' => $tableName,
            'Item' => [
                'id' => ['S' => 'test-id'],
                'sort' => ['N' => '123'],
                'data' => ['S' => 'some-data'],
            ],
        ];

        // Use reflection to call the private method for testing
        $reflection = new \ReflectionClass($this->protocol);
        $method = $reflection->getMethod('prepareSingleItemRequest');
        $method->setAccessible(true);

        $result = $method->invoke($this->protocol, $request);
        $this->assertArrayHasKey('Item', $result);
    }

    public function testBatchGetItemKeyValidation(): void
    {
        $tableName = 'TestTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
        ];

        $this->protocol->cacheKeySchema($tableName, $keySchema);

        $request = [
            'RequestItems' => [
                $tableName => [
                    'Keys' => [
                        ['id' => ['S' => 'test-id-1']],
                        ['id' => ['S' => 'test-id-2']],
                    ],
                ],
            ],
        ];

        // Use reflection to call the private method for testing
        $reflection = new \ReflectionClass($this->protocol);
        $method = $reflection->getMethod('prepareBatchGetRequest');
        $method->setAccessible(true);

        $result = $method->invoke($this->protocol, $request);
        $this->assertArrayHasKey('RequestItems', $result);
    }

    public function testBatchWriteItemKeyValidation(): void
    {
        $tableName = 'TestTable';
        $keySchema = [
            'HashKeyElement' => ['AttributeName' => 'id', 'AttributeType' => 'S'],
        ];

        $this->protocol->cacheKeySchema($tableName, $keySchema);

        $request = [
            'RequestItems' => [
                $tableName => [
                    [
                        'PutRequest' => [
                            'Item' => [
                                'id' => ['S' => 'test-id'],
                                'data' => ['S' => 'some-data'],
                            ],
                        ],
                    ],
                    [
                        'DeleteRequest' => [
                            'Key' => ['id' => ['S' => 'test-id-2']],
                        ],
                    ],
                ],
            ],
        ];

        // Use reflection to call the private method for testing
        $reflection = new \ReflectionClass($this->protocol);
        $method = $reflection->getMethod('prepareBatchWriteRequest');
        $method->setAccessible(true);

        $result = $method->invoke($this->protocol, $request);
        $this->assertArrayHasKey('RequestItems', $result);
    }

    public function testNoValidationWhenKeySchemaNotCached(): void
    {
        // Don't cache any key schema
        $request = [
            'TableName' => 'UnknownTable',
            'Key' => ['any_key' => ['S' => 'any-value']],
        ];

        // Use reflection to call the private method for testing
        $reflection = new \ReflectionClass($this->protocol);
        $method = $reflection->getMethod('prepareSingleItemRequest');
        $method->setAccessible(true);

        // Should not throw exception when key schema is not available
        $result = $method->invoke($this->protocol, $request);
        $this->assertArrayHasKey('Key', $result);
    }
}
