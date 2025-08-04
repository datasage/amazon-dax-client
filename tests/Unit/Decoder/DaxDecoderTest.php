<?php

declare(strict_types=1);

namespace Dax\Tests\Unit\Decoder;

use Dax\Decoder\DaxDecoder;
use Dax\Exception\DaxException;
use CBOR\CBORObject;
use CBOR\Decoder;
use CBOR\StringStream;
use CBOR\MapObject;
use CBOR\ListObject;
use CBOR\TextStringObject;
use CBOR\UnsignedIntegerObject;
use CBOR\NegativeIntegerObject;
use CBOR\OtherObject\TrueObject;
use CBOR\OtherObject\FalseObject;
use CBOR\OtherObject\NullObject;
use CBOR\Tag;
use CBOR\Tag\GenericTag;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for DaxDecoder
 */
class DaxDecoderTest extends TestCase
{
    private DaxDecoder $decoder;

    protected function setUp(): void
    {
        $this->decoder = new DaxDecoder();
    }

    public function testDecodeResponseWithEmptyResponse(): void
    {
        $result = $this->decoder->decodeResponse('GetItem', '');

        $this->assertEquals([], $result);
    }

    public function testDecodeResponseThrowsExceptionOnInvalidCbor(): void
    {
        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Failed to decode DAX response');

        // Invalid CBOR data - use binary data that will cause CBOR parsing to fail
        $invalidCbor = "\xFF\xFF\xFF\xFF"; // Invalid CBOR bytes
        $this->decoder->decodeResponse('GetItem', $invalidCbor);
    }

    public function testDecodeResponseThrowsExceptionOnNonArrayResult(): void
    {
        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Invalid response format: expected array');

        // Create Go protocol format: [CBOR_ERROR_ARRAY][CBOR_RESPONSE_DATA]
        // Empty error array (no error)
        $errorArray = ListObject::create();
        
        // Create CBOR that decodes to a string instead of array
        $textObject = TextStringObject::create('not an array');
        $cborData = (string) $errorArray . (string) $textObject;

        $this->decoder->decodeResponse('GetItem', $cborData);
    }

    public function testCborObjectToArrayWithTextString(): void
    {
        $textObject = TextStringObject::create('test string');
        $result = $this->decoder->cborObjectToArray($textObject);

        $this->assertEquals('test string', $result);
    }

    public function testCborObjectToArrayWithUnsignedInteger(): void
    {
        $intObject = UnsignedIntegerObject::create(42);
        $result = $this->decoder->cborObjectToArray($intObject);

        $this->assertEquals(42, $result);
    }

    public function testCborObjectToArrayWithNegativeInteger(): void
    {
        $intObject = NegativeIntegerObject::create(-42);
        $result = $this->decoder->cborObjectToArray($intObject);

        $this->assertEquals(-42, $result);
    }

    public function testCborObjectToArrayWithTrue(): void
    {
        $trueObject = TrueObject::create();
        $result = $this->decoder->cborObjectToArray($trueObject);

        $this->assertTrue($result);
    }

    public function testCborObjectToArrayWithFalse(): void
    {
        $falseObject = FalseObject::create();
        $result = $this->decoder->cborObjectToArray($falseObject);

        $this->assertFalse($result);
    }

    public function testCborObjectToArrayWithNull(): void
    {
        $nullObject = NullObject::create();
        $result = $this->decoder->cborObjectToArray($nullObject);

        $this->assertNull($result);
    }

    public function testCborObjectToArrayWithListObject(): void
    {
        $listObject = ListObject::create();
        $listObject->add(TextStringObject::create('item1'));
        $listObject->add(TextStringObject::create('item2'));

        $result = $this->decoder->cborObjectToArray($listObject);

        $this->assertEquals(['item1', 'item2'], $result);
    }

    public function testCborObjectToArrayWithMapObject(): void
    {
        $mapObject = MapObject::create();
        $mapObject->add(TextStringObject::create('key1'), TextStringObject::create('value1'));
        $mapObject->add(TextStringObject::create('key2'), UnsignedIntegerObject::create(42));

        $result = $this->decoder->cborObjectToArray($mapObject);

        $this->assertEquals(['key1' => 'value1', 'key2' => 42], $result);
    }

    public function testCborObjectToArrayWithTaggedObject(): void
    {
        // Create a tagged object for string set (SS)
        $listObject = ListObject::create();
        $listObject->add(TextStringObject::create('string1'));
        $listObject->add(TextStringObject::create('string2'));

        $taggedObject = new GenericTag(25, pack('n', 3321), $listObject); // TAG_DDB_STRING_SET = 3321

        $result = $this->decoder->cborObjectToArray($taggedObject);

        $this->assertEquals(['SS' => ['string1', 'string2']], $result);
    }

    public function testCborObjectToArrayWithUnknownObject(): void
    {
        // Create a mock CBOR object that doesn't match any known types
        $mockObject = $this->createMock(CBORObject::class);
        $mockObject->method('__toString')->willReturn('mock object string');

        $result = $this->decoder->cborObjectToArray($mockObject);

        $this->assertEquals('mock object string', $result);
    }

    public function testConvertResponseAttributeValuesWithSimpleArray(): void
    {
        $response = [
            'name' => 'John',
            'age' => 30,
            'active' => true,
        ];

        $result = $this->decoder->convertResponseAttributeValues($response);

        $this->assertEquals($response, $result);
    }

    public function testConvertResponseAttributeValuesWithDynamoDbAttributes(): void
    {
        $response = [
            'name' => ['S' => 'John Doe'],
            'age' => ['N' => '30'],
            'active' => ['BOOL' => true],
            'metadata' => ['NULL' => true],
        ];

        $result = $this->decoder->convertResponseAttributeValues($response);

        $expected = [
            'name' => 'John Doe',
            'age' => 30,
            'active' => true,
            'metadata' => null,
        ];

        $this->assertEquals($expected, $result);
    }

    public function testConvertResponseAttributeValuesWithNestedStructures(): void
    {
        $response = [
            'user' => [
                'M' => [
                    'name' => ['S' => 'John'],
                    'preferences' => [
                        'L' => [
                            ['S' => 'pref1'],
                            ['S' => 'pref2'],
                        ],
                    ],
                ],
            ],
        ];

        $result = $this->decoder->convertResponseAttributeValues($response);

        $expected = [
            'user' => [
                'name' => 'John',
                'preferences' => ['pref1', 'pref2'],
            ],
        ];

        $this->assertEquals($expected, $result);
    }

    public function testConvertFromDynamoDbAttributeWithString(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['S' => 'test string']);

        $this->assertEquals('test string', $result);
    }

    public function testConvertFromDynamoDbAttributeWithIntegerNumber(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['N' => '42']);

        $this->assertEquals(42, $result);
    }

    public function testConvertFromDynamoDbAttributeWithFloatNumber(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['N' => '3.14']);

        $this->assertEquals(3.14, $result);
    }

    public function testConvertFromDynamoDbAttributeWithNonNumericNumber(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['N' => 'not-a-number']);

        $this->assertEquals('not-a-number', $result);
    }

    public function testConvertFromDynamoDbAttributeWithBinary(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['B' => 'binary data']);

        $this->assertEquals('binary data', $result);
    }

    public function testConvertFromDynamoDbAttributeWithStringSet(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['SS' => ['string1', 'string2']]);

        $this->assertEquals(['string1', 'string2'], $result);
    }

    public function testConvertFromDynamoDbAttributeWithNumberSet(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['NS' => ['1', '2', '3']]);

        $this->assertEquals(['1', '2', '3'], $result);
    }

    public function testConvertFromDynamoDbAttributeWithBinarySet(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['BS' => ['binary1', 'binary2']]);

        $this->assertEquals(['binary1', 'binary2'], $result);
    }

    public function testConvertFromDynamoDbAttributeWithMap(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $mapData = [
            'name' => ['S' => 'John'],
            'age' => ['N' => '30'],
        ];

        $result = $method->invoke($this->decoder, ['M' => $mapData]);

        $expected = [
            'name' => 'John',
            'age' => 30,
        ];

        $this->assertEquals($expected, $result);
    }

    public function testConvertFromDynamoDbAttributeWithList(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $listData = [
            ['S' => 'item1'],
            ['S' => 'item2'],
            ['N' => '42'],
        ];

        $result = $method->invoke($this->decoder, ['L' => $listData]);

        $this->assertEquals(['item1', 'item2', 42], $result);
    }

    public function testConvertFromDynamoDbAttributeWithNull(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['NULL' => true]);

        $this->assertNull($result);
    }

    public function testConvertFromDynamoDbAttributeWithBoolean(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['BOOL' => true]);

        $this->assertTrue($result);
    }

    public function testConvertFromDynamoDbAttributeWithUnknownType(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('convertFromDynamoDbAttribute');
        $method->setAccessible(true);

        $result = $method->invoke($this->decoder, ['UNKNOWN' => 'some value']);

        $this->assertEquals('some value', $result);
    }

    public function testIsDynamoDbAttributeWithValidAttribute(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('isDynamoDbAttribute');
        $method->setAccessible(true);

        $this->assertTrue($method->invoke($this->decoder, ['S' => 'value']));
        $this->assertTrue($method->invoke($this->decoder, ['N' => '123']));
        $this->assertTrue($method->invoke($this->decoder, ['BOOL' => true]));
        $this->assertTrue($method->invoke($this->decoder, ['NULL' => true]));
        $this->assertTrue($method->invoke($this->decoder, ['SS' => ['a', 'b']]));
        $this->assertTrue($method->invoke($this->decoder, ['NS' => ['1', '2']]));
        $this->assertTrue($method->invoke($this->decoder, ['BS' => ['x', 'y']]));
        $this->assertTrue($method->invoke($this->decoder, ['M' => []]));
        $this->assertTrue($method->invoke($this->decoder, ['L' => []]));
    }

    public function testIsDynamoDbAttributeWithInvalidAttribute(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('isDynamoDbAttribute');
        $method->setAccessible(true);

        $this->assertFalse($method->invoke($this->decoder, ['INVALID' => 'value']));
        $this->assertFalse($method->invoke($this->decoder, ['S' => 'value', 'N' => '123'])); // Multiple keys
        $this->assertFalse($method->invoke($this->decoder, [])); // Empty array
    }

    public function testDecodeDynamoDbSetWithStringSet(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDynamoDbSet');
        $method->setAccessible(true);

        $listObject = ListObject::create();
        $listObject->add(TextStringObject::create('string1'));
        $listObject->add(TextStringObject::create('string2'));

        $result = $method->invoke($this->decoder, 'SS', $listObject);

        $this->assertEquals(['SS' => ['string1', 'string2']], $result);
    }

    public function testDecodeDynamoDbSetWithNumberSet(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDynamoDbSet');
        $method->setAccessible(true);

        $listObject = ListObject::create();
        $listObject->add(TextStringObject::create('1'));
        $listObject->add(TextStringObject::create('2'));

        $result = $method->invoke($this->decoder, 'NS', $listObject);

        $this->assertEquals(['NS' => ['1', '2']], $result);
    }

    public function testDecodeDynamoDbSetWithBinarySet(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDynamoDbSet');
        $method->setAccessible(true);

        $listObject = ListObject::create();
        $listObject->add(TextStringObject::create('binary1'));
        $listObject->add(TextStringObject::create('binary2'));

        $result = $method->invoke($this->decoder, 'BS', $listObject);

        $this->assertEquals(['BS' => ['binary1', 'binary2']], $result);
    }

    public function testDecodeDynamoDbSetThrowsExceptionOnNonListObject(): void
    {
        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Expected ListObject for DynamoDB set');

        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDynamoDbSet');
        $method->setAccessible(true);

        $textObject = TextStringObject::create('not a list');
        $method->invoke($this->decoder, 'SS', $textObject);
    }

    public function testDecodeDocumentPathOrdinal(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDocumentPathOrdinal');
        $method->setAccessible(true);

        $ordinalObject = UnsignedIntegerObject::create(123);
        $result = $method->invoke($this->decoder, $ordinalObject);

        $this->assertEquals(['_document_path_ordinal' => 123], $result);
    }

    public function testDecodeDaxTaggedObjectWithStringSet(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDaxTaggedObject');
        $method->setAccessible(true);

        $listObject = ListObject::create();
        $listObject->add(TextStringObject::create('string1'));

        $taggedObject = new GenericTag(25, pack('n', 3321), $listObject); // TAG_DDB_STRING_SET = 3321

        $result = $method->invoke($this->decoder, $taggedObject);

        $this->assertEquals(['SS' => ['string1']], $result);
    }

    public function testDecodeDaxTaggedObjectWithNumberSet(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDaxTaggedObject');
        $method->setAccessible(true);

        $listObject = ListObject::create();
        $listObject->add(TextStringObject::create('1'));

        $taggedObject = new GenericTag(25, pack('n', 3322), $listObject); // TAG_DDB_NUMBER_SET = 3322

        $result = $method->invoke($this->decoder, $taggedObject);

        $this->assertEquals(['NS' => ['1']], $result);
    }

    public function testDecodeDaxTaggedObjectWithBinarySet(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDaxTaggedObject');
        $method->setAccessible(true);

        $listObject = ListObject::create();
        $listObject->add(TextStringObject::create('binary1'));

        $taggedObject = new GenericTag(25, pack('n', 3323), $listObject); // TAG_DDB_BINARY_SET = 3323

        $result = $method->invoke($this->decoder, $taggedObject);

        $this->assertEquals(['BS' => ['binary1']], $result);
    }

    public function testDecodeDaxTaggedObjectWithDocumentPathOrdinal(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDaxTaggedObject');
        $method->setAccessible(true);

        $ordinalObject = UnsignedIntegerObject::create(456);
        $taggedObject = new GenericTag(25, pack('n', 3324), $ordinalObject); // TAG_DDB_DOCUMENT_PATH_ORDINAL = 3324

        $result = $method->invoke($this->decoder, $taggedObject);

        $this->assertEquals(['_document_path_ordinal' => 456], $result);
    }

    public function testDecodeDaxTaggedObjectWithUnknownTag(): void
    {
        $reflection = new \ReflectionClass($this->decoder);
        $method = $reflection->getMethod('decodeDaxTaggedObject');
        $method->setAccessible(true);

        $textObject = TextStringObject::create('unknown tagged value');
        $taggedObject = new GenericTag(25, pack('n', 999), $textObject); // Unknown tag 999

        $result = $method->invoke($this->decoder, $taggedObject);

        $this->assertEquals('unknown tagged value', $result);
    }

    public function testDecodeResponseWithValidCborArray(): void
    {
        // Create Go protocol format: [CBOR_ERROR_ARRAY][CBOR_RESPONSE_DATA]
        // Empty error array (no error)
        $errorArray = ListObject::create();
        
        // Response data
        $mapObject = MapObject::create();
        $mapObject->add(TextStringObject::create('name'), TextStringObject::create('John'));
        $mapObject->add(TextStringObject::create('age'), UnsignedIntegerObject::create(30));

        $cborData = (string) $errorArray . (string) $mapObject;

        $result = $this->decoder->decodeResponse('GetItem', $cborData);

        $this->assertEquals(['name' => 'John', 'age' => 30], $result);
    }

    public function testDecodeResponseWithDynamoDbAttributes(): void
    {
        // Create Go protocol format: [CBOR_ERROR_ARRAY][CBOR_RESPONSE_DATA]
        // Empty error array (no error)
        $errorArray = ListObject::create();
        
        // Create CBOR with DynamoDB attributes
        $mapObject = MapObject::create();

        // Add a DynamoDB string attribute
        $stringAttr = MapObject::create();
        $stringAttr->add(TextStringObject::create('S'), TextStringObject::create('John Doe'));
        $mapObject->add(TextStringObject::create('name'), $stringAttr);

        // Add a DynamoDB number attribute
        $numberAttr = MapObject::create();
        $numberAttr->add(TextStringObject::create('N'), TextStringObject::create('30'));
        $mapObject->add(TextStringObject::create('age'), $numberAttr);

        $cborData = (string) $errorArray . (string) $mapObject;

        $result = $this->decoder->decodeResponse('GetItem', $cborData);

        $expected = [
            'name' => 'John Doe',
            'age' => 30,
        ];

        $this->assertEquals($expected, $result);
    }
}
