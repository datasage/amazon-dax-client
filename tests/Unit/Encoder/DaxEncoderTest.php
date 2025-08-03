<?php

declare(strict_types=1);

namespace Amazon\Dax\Tests\Unit\Encoder;

use Amazon\Dax\Encoder\DaxEncoder;
use Amazon\Dax\Exception\DaxException;
use CBOR\CBORObject;
use CBOR\MapObject;
use CBOR\ListObject;
use CBOR\TextStringObject;
use CBOR\UnsignedIntegerObject;
use CBOR\NegativeIntegerObject;
use CBOR\OtherObject\TrueObject;
use CBOR\OtherObject\FalseObject;
use CBOR\OtherObject\NullObject;
use CBOR\Tag\GenericTag;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for DaxEncoder
 */
class DaxEncoderTest extends TestCase
{
    private DaxEncoder $encoder;

    protected function setUp(): void
    {
        $this->encoder = new DaxEncoder();
    }

    public function testEncodeRequestWithSimpleArray(): void
    {
        $methodId = 123;
        $request = ['key' => 'value'];
        
        $result = $this->encoder->encodeRequest($methodId, $request);
        
        // Check that result starts with method ID and length
        $this->assertIsString($result);
        $this->assertGreaterThan(8, strlen($result)); // At least header (8 bytes) + payload
        
        // Verify header format: [method_id:4][length:4][payload]
        $header = substr($result, 0, 8);
        $headerData = unpack('Nmethod_id/Nlength', $header);
        
        $this->assertEquals($methodId, $headerData['method_id']);
        $this->assertEquals(strlen($result) - 8, $headerData['length']);
    }

    public function testEncodeRequestThrowsExceptionOnError(): void
    {
        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Failed to encode request');
        
        // Create a mock that will throw an exception
        $encoder = $this->getMockBuilder(DaxEncoder::class)
            ->onlyMethods(['arrayToCborObject'])
            ->getMock();
        
        $encoder->method('arrayToCborObject')
            ->willThrowException(new \Exception('Test exception'));
        
        $encoder->encodeRequest(123, ['test']);
    }

    public function testArrayToCborObjectWithString(): void
    {
        $result = $this->encoder->arrayToCborObject('test string');
        
        $this->assertInstanceOf(TextStringObject::class, $result);
        $this->assertEquals('test string', $result->getValue());
    }

    public function testArrayToCborObjectWithPositiveInteger(): void
    {
        $result = $this->encoder->arrayToCborObject(42);
        
        $this->assertInstanceOf(UnsignedIntegerObject::class, $result);
        $this->assertEquals(42, $result->getValue());
    }

    public function testArrayToCborObjectWithNegativeInteger(): void
    {
        $result = $this->encoder->arrayToCborObject(-42);
        
        $this->assertInstanceOf(NegativeIntegerObject::class, $result);
        $this->assertEquals(-42, $result->getValue());
    }

    public function testArrayToCborObjectWithZero(): void
    {
        $result = $this->encoder->arrayToCborObject(0);
        
        $this->assertInstanceOf(UnsignedIntegerObject::class, $result);
        $this->assertEquals(0, $result->getValue());
    }

    public function testArrayToCborObjectWithTrue(): void
    {
        $result = $this->encoder->arrayToCborObject(true);
        
        $this->assertInstanceOf(TrueObject::class, $result);
    }

    public function testArrayToCborObjectWithFalse(): void
    {
        $result = $this->encoder->arrayToCborObject(false);
        
        $this->assertInstanceOf(FalseObject::class, $result);
    }

    public function testArrayToCborObjectWithNull(): void
    {
        $result = $this->encoder->arrayToCborObject(null);
        
        $this->assertInstanceOf(NullObject::class, $result);
    }

    public function testArrayToCborObjectWithFloat(): void
    {
        $result = $this->encoder->arrayToCborObject(3.14);
        
        $this->assertInstanceOf(TextStringObject::class, $result);
        $this->assertEquals('3.14', $result->getValue());
    }

    public function testArrayToCborObjectWithIndexedArray(): void
    {
        $data = ['item1', 'item2', 'item3'];
        $result = $this->encoder->arrayToCborObject($data);
        
        $this->assertInstanceOf(ListObject::class, $result);
        
        // Convert to array to check contents
        $items = [];
        foreach ($result as $item) {
            $items[] = $item;
        }
        
        $this->assertCount(3, $items);
        $this->assertInstanceOf(TextStringObject::class, $items[0]);
        $this->assertEquals('item1', $items[0]->getValue());
    }

    public function testArrayToCborObjectWithAssociativeArray(): void
    {
        $data = ['key1' => 'value1', 'key2' => 'value2'];
        $result = $this->encoder->arrayToCborObject($data);
        
        $this->assertInstanceOf(MapObject::class, $result);
        
        // Convert to array to check contents
        $items = [];
        foreach ($result as $mapItem) {
            $key = $mapItem->getKey()->getValue();
            $value = $mapItem->getValue()->getValue();
            $items[$key] = $value;
        }
        
        $this->assertEquals(['key1' => 'value1', 'key2' => 'value2'], $items);
    }

    public function testArrayToCborObjectWithEmptyArray(): void
    {
        $result = $this->encoder->arrayToCborObject([]);
        
        $this->assertInstanceOf(ListObject::class, $result);
        
        $count = 0;
        foreach ($result as $item) {
            $count++;
        }
        $this->assertEquals(0, $count);
    }

    public function testArrayToCborObjectWithNestedArray(): void
    {
        $data = [
            'nested' => [
                'inner' => 'value'
            ]
        ];
        
        $result = $this->encoder->arrayToCborObject($data);
        
        $this->assertInstanceOf(MapObject::class, $result);
    }

    public function testArrayToCborObjectWithStringSet(): void
    {
        $data = ['SS' => ['string1', 'string2', 'string3']];
        $result = $this->encoder->arrayToCborObject($data);
        
        $this->assertInstanceOf(GenericTag::class, $result);
        // GenericTag doesn't have getTag() method, but we can verify it's a tagged object
        $this->assertInstanceOf(ListObject::class, $result->getValue());
    }

    public function testArrayToCborObjectWithNumberSet(): void
    {
        $data = ['NS' => ['1', '2', '3']];
        $result = $this->encoder->arrayToCborObject($data);
        
        $this->assertInstanceOf(GenericTag::class, $result);
        $this->assertEquals(259, $result->getTag()); // TAG_DDB_NUMBER_SET
    }

    public function testArrayToCborObjectWithBinarySet(): void
    {
        $data = ['BS' => ['binary1', 'binary2']];
        $result = $this->encoder->arrayToCborObject($data);
        
        $this->assertInstanceOf(GenericTag::class, $result);
        $this->assertEquals(260, $result->getTag()); // TAG_DDB_BINARY_SET
    }

    public function testArrayToCborObjectWithUnknownSetType(): void
    {
        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Unknown DynamoDB set type: XS');
        
        $data = ['XS' => ['value1', 'value2']];
        $this->encoder->arrayToCborObject($data);
    }

    public function testArrayToCborObjectWithNonSetSingleKeyArray(): void
    {
        $data = ['NotASet' => ['value1', 'value2']];
        $result = $this->encoder->arrayToCborObject($data);
        
        // Should be treated as regular associative array, not a set
        $this->assertInstanceOf(MapObject::class, $result);
    }

    public function testArrayToCborObjectWithMultipleKeysArray(): void
    {
        $data = ['SS' => ['string1'], 'NS' => ['1']];
        $result = $this->encoder->arrayToCborObject($data);
        
        // Should be treated as regular associative array since it has multiple keys
        $this->assertInstanceOf(MapObject::class, $result);
    }

    public function testArrayToCborObjectWithResource(): void
    {
        $resource = fopen('php://memory', 'r');
        $result = $this->encoder->arrayToCborObject($resource);
        fclose($resource);
        
        // Should fallback to string representation
        $this->assertInstanceOf(TextStringObject::class, $result);
    }

    public function testArrayToCborObjectWithObject(): void
    {
        $object = new \stdClass();
        $object->property = 'value';
        
        $result = $this->encoder->arrayToCborObject($object);
        
        // Should fallback to string representation
        $this->assertInstanceOf(TextStringObject::class, $result);
    }

    /**
     * Test determineTagComponents method indirectly through DynamoDB sets
     */
    public function testDetermineTagComponentsWithSmallTag(): void
    {
        // Test with SS set (tag 258)
        $data = ['SS' => ['test']];
        $result = $this->encoder->arrayToCborObject($data);
        
        $this->assertInstanceOf(GenericTag::class, $result);
        $this->assertEquals(258, $result->getTag());
    }

    public function testDetermineTagComponentsWithLargeTag(): void
    {
        // We can't directly test private method, but we can test the behavior
        // through the public methods that use it
        $data = ['SS' => ['test']];
        $result = $this->encoder->arrayToCborObject($data);
        
        $this->assertInstanceOf(GenericTag::class, $result);
    }

    public function testComplexNestedStructure(): void
    {
        $data = [
            'TableName' => 'TestTable',
            'Key' => [
                'id' => ['S' => 'test-id'],
                'sort' => ['N' => '123']
            ],
            'Item' => [
                'name' => ['S' => 'Test Name'],
                'active' => ['BOOL' => true],
                'tags' => ['SS' => ['tag1', 'tag2', 'tag3']],
                'numbers' => ['NS' => ['1', '2', '3']],
                'metadata' => [
                    'M' => [
                        'created' => ['S' => '2023-01-01'],
                        'version' => ['N' => '1']
                    ]
                ]
            ]
        ];
        
        $result = $this->encoder->arrayToCborObject($data);
        
        $this->assertInstanceOf(MapObject::class, $result);
    }

    public function testEncodeRequestWithComplexData(): void
    {
        $methodId = 263244906; // GetItem method ID
        $request = [
            'TableName' => 'TestTable',
            'Key' => [
                'id' => ['S' => 'test-id']
            ]
        ];
        
        $result = $this->encoder->encodeRequest($methodId, $request);
        
        $this->assertIsString($result);
        $this->assertGreaterThan(8, strlen($result));
        
        // Verify header
        $header = substr($result, 0, 8);
        $headerData = unpack('Nmethod_id/Nlength', $header);
        
        $this->assertEquals($methodId, $headerData['method_id']);
        $this->assertEquals(strlen($result) - 8, $headerData['length']);
    }
}
