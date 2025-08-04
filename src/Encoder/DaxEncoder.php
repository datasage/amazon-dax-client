<?php

declare(strict_types=1);

namespace Dax\Encoder;

use Dax\Exception\DaxException;
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

/**
 * DAX Protocol Encoder
 *
 * Handles encoding of requests and data structures for DAX protocol communication
 */
class DaxEncoder
{
    // DAX service ID constant
    private const DAX_SERVICE_ID = 1;
    
    // DAX CBOR tags for DynamoDB data types
    private const TAG_DDB_STRING_SET = 258;
    private const TAG_DDB_NUMBER_SET = 259;
    private const TAG_DDB_BINARY_SET = 260;

    /**
     * Encode a request for transmission
     *
     * @param int $methodId Method ID
     * @param array $request Request parameters
     * @return string Encoded request
     * @throws DaxException
     */
    public function encodeRequest(int $methodId, array $request): string
    {
        try {
            // Create CBOR objects for service ID and method ID
            $serviceIdObject = UnsignedIntegerObject::create(self::DAX_SERVICE_ID);
            $methodIdObject = $methodId >= 0 ? 
                UnsignedIntegerObject::create($methodId) : 
                NegativeIntegerObject::create($methodId);
            
            // Convert request parameters to CBOR object
            $requestObject = $this->arrayToCborObject($request);
            
            // Encode all components as CBOR
            return (string) $serviceIdObject . (string) $methodIdObject . (string) $requestObject;
        } catch (\Exception $e) {
            throw new DaxException('Failed to encode request: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Convert PHP array to CBOR object
     *
     * @param mixed $data Data to convert
     * @return CBORObject CBOR object
     */
    public function arrayToCborObject($data): CBORObject
    {
        if (is_array($data)) {
            // Check if it's a DynamoDB set type (SS, NS, BS)
            if ($this->isDynamoDbSet($data)) {
                return $this->encodeDynamoDbSet($data);
            }

            // Check if it's an associative array (map) or indexed array (list)
            // Handle empty array as ListObject
            if (empty($data) || array_keys($data) === range(0, count($data) - 1)) {
                // Indexed array - create ListObject
                $listObject = ListObject::create();
                foreach ($data as $item) {
                    $listObject->add($this->arrayToCborObject($item));
                }
                return $listObject;
            } else {
                // Associative array - create MapObject
                $mapObject = MapObject::create();
                foreach ($data as $key => $value) {
                    $keyObject = $this->arrayToCborObject($key);
                    $valueObject = $this->arrayToCborObject($value);
                    $mapObject->add($keyObject, $valueObject);
                }
                return $mapObject;
            }
        } elseif (is_string($data)) {
            return TextStringObject::create($data);
        } elseif (is_int($data)) {
            return $data >= 0 ? UnsignedIntegerObject::create($data) : NegativeIntegerObject::create($data);
        } elseif (is_bool($data)) {
            return $data ? TrueObject::create() : FalseObject::create();
        } elseif (is_null($data)) {
            return NullObject::create();
        } elseif (is_float($data)) {
            // For floats, we'll convert to string and back to maintain precision
            return TextStringObject::create((string) $data);
        } else {
            // Fallback to string representation for objects and other types
            if (is_object($data)) {
                return TextStringObject::create(json_encode($data) ?: 'Object');
            }
            return TextStringObject::create((string) $data);
        }
    }

    /**
     * Check if an array represents a DynamoDB set (SS, NS, BS)
     *
     * @param array $data Array to check
     * @return bool True if it's a DynamoDB set
     */
    private function isDynamoDbSet(array $data): bool
    {
        if (count($data) !== 1) {
            return false;
        }

        $type = array_keys($data)[0];
        return in_array($type, ['SS', 'NS', 'BS']);
    }

    /**
     * Encode a DynamoDB set with appropriate DAX CBOR tag
     *
     * @param array $data DynamoDB set data
     * @return CBORObject Tagged CBOR object
     */
    private function encodeDynamoDbSet(array $data): CBORObject
    {
        $type = array_keys($data)[0];
        $values = $data[$type];

        // Determine the appropriate tag
        switch ($type) {
            case 'SS':
                $tag = self::TAG_DDB_STRING_SET;
                break;
            case 'NS':
                $tag = self::TAG_DDB_NUMBER_SET;
                break;
            case 'BS':
                $tag = self::TAG_DDB_BINARY_SET;
                break;
            default:
                throw new DaxException("Unknown DynamoDB set type: {$type}");
        }

        // Create a list of the set values
        $listObject = ListObject::create();
        foreach ($values as $value) {
            $listObject->add($this->arrayToCborObject($value));
        }

        // Create a generic tag with the appropriate tag number
        // Manually determine components to avoid hex2bin issues
        [$additionalInformation, $data] = $this->determineTagComponents($tag);

        return new GenericTag($additionalInformation, $data, $listObject);
    }

    /**
     * Determine CBOR tag components for a given tag number
     *
     * @param int $tag Tag number
     * @return array [additionalInformation, data]
     */
    private function determineTagComponents(int $tag): array
    {
        if ($tag < 0) {
            throw new DaxException('Tag value must be a positive integer.');
        }

        if ($tag < 24) {
            return [$tag, null];
        } elseif ($tag < 0xFF) {
            return [24, pack('C', $tag)];
        } elseif ($tag < 0xFFFF) {
            return [25, pack('n', $tag)];
        } elseif ($tag < 0xFFFFFFFF) {
            return [26, pack('N', $tag)];
        } else {
            return [27, pack('J', $tag)];
        }
    }
}
