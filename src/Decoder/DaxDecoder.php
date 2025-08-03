<?php

declare(strict_types=1);

namespace Dax\Decoder;

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

/**
 * DAX Protocol Decoder
 * 
 * Handles decoding of responses and data structures from DAX protocol communication
 */
class DaxDecoder
{
    // DAX CBOR tags for DynamoDB data types
    private const TAG_DDB_STRING_SET = 258;
    private const TAG_DDB_NUMBER_SET = 259;
    private const TAG_DDB_BINARY_SET = 260;
    private const TAG_DDB_DOCUMENT_PATH_ORDINAL = 261;

    /**
     * Decode a response
     *
     * @param string $operation Operation name
     * @param string $response Raw response data
     * @return array Decoded response
     * @throws DaxException
     */
    public function decodeResponse(string $operation, string $response): array
    {
        if (empty($response)) {
            return [];
        }
        
        try {
            // Use CBOR decoding for the response
            $decoder = Decoder::create();
            $stream = StringStream::create($response);
            $cborObject = $decoder->decode($stream);
            
            // Convert CBOR object to PHP array
            $decoded = $this->cborObjectToArray($cborObject);
            
            if (!is_array($decoded)) {
                throw new DaxException('Invalid response format: expected array');
            }
            
            return $this->convertResponseAttributeValues($decoded);
        } catch (\Exception $e) {
            throw new DaxException('Failed to decode DAX response: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Convert CBOR object to PHP array
     *
     * @param \CBOR\CBORObject $cborObject CBOR object
     * @return mixed PHP value
     */
    public function cborObjectToArray(\CBOR\CBORObject $cborObject)
    {
        if ($cborObject instanceof Tag) {
            return $this->decodeDaxTaggedObject($cborObject);
        } elseif ($cborObject instanceof MapObject) {
            $result = [];
            // Iterate over MapItem objects
            foreach ($cborObject as $mapItem) {
                $key = $mapItem->getKey();
                $value = $mapItem->getValue();
                $phpKey = $this->cborObjectToArray($key);
                $phpValue = $this->cborObjectToArray($value);
                $result[$phpKey] = $phpValue;
            }
            return $result;
        } elseif ($cborObject instanceof ListObject) {
            $result = [];
            foreach ($cborObject as $item) {
                $result[] = $this->cborObjectToArray($item);
            }
            return $result;
        } elseif ($cborObject instanceof TextStringObject) {
            return $cborObject->getValue();
        } elseif ($cborObject instanceof UnsignedIntegerObject) {
            return $cborObject->getValue();
        } elseif ($cborObject instanceof NegativeIntegerObject) {
            return $cborObject->getValue();
        } elseif ($cborObject instanceof TrueObject) {
            return true;
        } elseif ($cborObject instanceof FalseObject) {
            return false;
        } elseif ($cborObject instanceof NullObject) {
            return null;
        } else {
            // Fallback - try to get normalized value
            return method_exists($cborObject, 'getNormalizedData') ? 
                $cborObject->getNormalizedData() : (string) $cborObject;
        }
    }

    /**
     * Convert response attribute values from DAX format
     *
     * @param array $response Response data
     * @return array Converted response
     */
    public function convertResponseAttributeValues(array $response): array
    {
        // Recursively convert attribute values in the response
        foreach ($response as $key => &$value) {
            if (is_array($value)) {
                if ($this->isDynamoDbAttribute($value)) {
                    $value = $this->convertFromDynamoDbAttribute($value);
                } else {
                    $value = $this->convertResponseAttributeValues($value);
                }
            }
        }
        
        return $response;
    }

    /**
     * Check if an array is a DynamoDB attribute
     *
     * @param array $value Array to check
     * @return bool True if it's a DynamoDB attribute
     */
    private function isDynamoDbAttribute(array $value): bool
    {
        if (count($value) !== 1) {
            return false;
        }
        
        $type = array_keys($value)[0];
        return in_array($type, ['S', 'N', 'B', 'SS', 'NS', 'BS', 'M', 'L', 'NULL', 'BOOL']);
    }

    /**
     * Convert from DynamoDB attribute format
     *
     * @param array $attribute DynamoDB attribute
     * @return mixed Converted value
     */
    private function convertFromDynamoDbAttribute(array $attribute)
    {
        $type = array_keys($attribute)[0];
        $value = $attribute[$type];
        
        switch ($type) {
            case 'S':
                return (string) $value;
            case 'N':
                return is_numeric($value) ? (strpos($value, '.') !== false ? (float) $value : (int) $value) : $value;
            case 'B':
                return $value; // Binary data
            case 'SS':
            case 'NS':
            case 'BS':
                return $value; // Sets
            case 'M':
                return $this->convertResponseAttributeValues($value);
            case 'L':
                return array_map([$this, 'convertFromDynamoDbAttribute'], $value);
            case 'NULL':
                return null;
            case 'BOOL':
                return (bool) $value;
            default:
                return $value;
        }
    }

    /**
     * Decode DAX-specific tagged CBOR objects
     *
     * @param Tag $taggedObject Tagged CBOR object
     * @return mixed Decoded value
     */
    private function decodeDaxTaggedObject(Tag $taggedObject)
    {
        // Extract tag number from the data field based on additional information
        $additionalInfo = $taggedObject->getAdditionalInformation();
        $data = $taggedObject->getData();
        
        if ($additionalInfo < 24) {
            $tag = $additionalInfo;
        } elseif ($additionalInfo === 24 && $data !== null) {
            $tag = unpack('C', $data)[1];
        } elseif ($additionalInfo === 25 && $data !== null) {
            $tag = unpack('n', $data)[1];
        } elseif ($additionalInfo === 26 && $data !== null) {
            $tag = unpack('N', $data)[1];
        } else {
            // Fallback - use additional info as tag
            $tag = $additionalInfo;
        }
        $value = $taggedObject->getValue();
        
        switch ($tag) {
            case self::TAG_DDB_STRING_SET:
                return $this->decodeDynamoDbSet('SS', $value);
            case self::TAG_DDB_NUMBER_SET:
                return $this->decodeDynamoDbSet('NS', $value);
            case self::TAG_DDB_BINARY_SET:
                return $this->decodeDynamoDbSet('BS', $value);
            case self::TAG_DDB_DOCUMENT_PATH_ORDINAL:
                return $this->decodeDocumentPathOrdinal($value);
            default:
                // For unknown tags, just return the decoded value
                return $this->cborObjectToArray($value);
        }
    }

    /**
     * Decode a DynamoDB set from CBOR
     *
     * @param string $setType Set type (SS, NS, BS)
     * @param CBORObject $value CBOR value
     * @return array DynamoDB set format
     */
    private function decodeDynamoDbSet(string $setType, CBORObject $value): array
    {
        if (!($value instanceof ListObject)) {
            throw new DaxException("Expected ListObject for DynamoDB set, got " . get_class($value));
        }
        
        $values = [];
        foreach ($value as $item) {
            $values[] = $this->cborObjectToArray($item);
        }
        
        return [$setType => $values];
    }

    /**
     * Decode a document path ordinal from CBOR
     *
     * @param CBORObject $value CBOR value
     * @return array Document path ordinal format
     */
    private function decodeDocumentPathOrdinal(CBORObject $value): array
    {
        $ordinal = $this->cborObjectToArray($value);
        return ['_document_path_ordinal' => $ordinal];
    }
}
