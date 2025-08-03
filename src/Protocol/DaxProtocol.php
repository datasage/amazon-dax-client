<?php

declare(strict_types=1);

namespace Amazon\Dax\Protocol;

use Amazon\Dax\Connection\DaxConnection;
use Amazon\Dax\Exception\DaxException;
use Amazon\Dax\Cache\KeySchemaCache;
use Amazon\Dax\Cache\AttributeListCache;

/**
 * Handles the DAX protocol communication
 */
class DaxProtocol
{
    private array $config;
    private ?KeySchemaCache $keySchemaCache;
    private ?AttributeListCache $attributeListCache;

    // DAX Method IDs (simplified subset from Python client)
    private const METHOD_IDS = [
        'GetItem' => 263244906,
        'PutItem' => 20969,
        'DeleteItem' => 7,
        'UpdateItem' => 10,
        'BatchGetItem' => 697851100,
        'BatchWriteItem' => 116217951,
        'Query' => 2,
        'Scan' => 3,
        'DefineKeySchema' => 681,
        'DefineAttributeList' => 656,
        'DefineAttributeListId' => 657
    ];

    /**
     * Constructor
     *
     * @param array $config Protocol configuration
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        $this->keySchemaCache = $config['key_schema_cache'] ?? null;
        $this->attributeListCache = $config['attribute_list_cache'] ?? null;
    }

    /**
     * Execute a request against a DAX connection
     *
     * @param DaxConnection $connection DAX connection
     * @param string $operation Operation name
     * @param array $request Request parameters
     * @return array Response
     * @throws DaxException
     */
    public function executeRequest(DaxConnection $connection, string $operation, array $request): array
    {
        $methodId = self::METHOD_IDS[$operation] ?? null;
        if ($methodId === null) {
            throw new DaxException("Unsupported operation: {$operation}");
        }

        // Prepare the request
        $preparedRequest = $this->prepareRequest($operation, $request);
        
        // Encode the request
        $encodedRequest = $this->encodeRequest($methodId, $preparedRequest);
        
        // Send the request
        $connection->send($encodedRequest);
        
        // Receive and decode the response
        $response = $this->receiveResponse($connection);
        
        // Decode the response
        return $this->decodeResponse($operation, $response);
    }

    /**
     * Prepare a request for encoding
     *
     * @param string $operation Operation name
     * @param array $request Request parameters
     * @return array Prepared request
     * @throws DaxException
     */
    private function prepareRequest(string $operation, array $request): array
    {
        switch ($operation) {
            case 'GetItem':
            case 'PutItem':
            case 'DeleteItem':
            case 'UpdateItem':
                return $this->prepareSingleItemRequest($request);
                
            case 'BatchGetItem':
                return $this->prepareBatchGetRequest($request);
                
            case 'BatchWriteItem':
                return $this->prepareBatchWriteRequest($request);
                
            case 'Query':
            case 'Scan':
                return $this->prepareQueryScanRequest($request);
                
            default:
                return $request;
        }
    }

    /**
     * Prepare a single item request
     *
     * @param array $request Request parameters
     * @return array Prepared request
     */
    private function prepareSingleItemRequest(array $request): array
    {
        // Ensure table name is present
        if (!isset($request['TableName'])) {
            throw new DaxException('TableName is required');
        }

        // Convert attribute values to DAX format
        if (isset($request['Key'])) {
            $request['Key'] = $this->convertAttributeValues($request['Key']);
        }
        
        if (isset($request['Item'])) {
            $request['Item'] = $this->convertAttributeValues($request['Item']);
        }

        return $request;
    }

    /**
     * Prepare a batch get request
     *
     * @param array $request Request parameters
     * @return array Prepared request
     */
    private function prepareBatchGetRequest(array $request): array
    {
        if (!isset($request['RequestItems'])) {
            throw new DaxException('RequestItems is required');
        }

        foreach ($request['RequestItems'] as $tableName => &$tableRequest) {
            if (isset($tableRequest['Keys'])) {
                foreach ($tableRequest['Keys'] as &$key) {
                    $key = $this->convertAttributeValues($key);
                }
            }
        }

        return $request;
    }

    /**
     * Prepare a batch write request
     *
     * @param array $request Request parameters
     * @return array Prepared request
     */
    private function prepareBatchWriteRequest(array $request): array
    {
        if (!isset($request['RequestItems'])) {
            throw new DaxException('RequestItems is required');
        }

        foreach ($request['RequestItems'] as $tableName => &$writeRequests) {
            foreach ($writeRequests as &$writeRequest) {
                if (isset($writeRequest['PutRequest']['Item'])) {
                    $writeRequest['PutRequest']['Item'] = $this->convertAttributeValues($writeRequest['PutRequest']['Item']);
                }
                if (isset($writeRequest['DeleteRequest']['Key'])) {
                    $writeRequest['DeleteRequest']['Key'] = $this->convertAttributeValues($writeRequest['DeleteRequest']['Key']);
                }
            }
        }

        return $request;
    }

    /**
     * Prepare a query or scan request
     *
     * @param array $request Request parameters
     * @return array Prepared request
     */
    private function prepareQueryScanRequest(array $request): array
    {
        if (!isset($request['TableName'])) {
            throw new DaxException('TableName is required');
        }

        // Convert attribute values in various conditions
        if (isset($request['ExclusiveStartKey'])) {
            $request['ExclusiveStartKey'] = $this->convertAttributeValues($request['ExclusiveStartKey']);
        }

        return $request;
    }

    /**
     * Convert DynamoDB attribute values to DAX format
     *
     * @param array $attributes Attribute values
     * @return array Converted attribute values
     */
    private function convertAttributeValues(array $attributes): array
    {
        // This is a simplified conversion
        // In a full implementation, this would handle all DynamoDB types
        $converted = [];
        
        foreach ($attributes as $name => $value) {
            if (is_array($value) && count($value) === 1) {
                // Already in DynamoDB format (e.g., ['S' => 'value'])
                $converted[$name] = $value;
            } else {
                // Convert simple values to DynamoDB format
                $converted[$name] = $this->convertSimpleValue($value);
            }
        }
        
        return $converted;
    }

    /**
     * Convert a simple value to DynamoDB attribute format
     *
     * @param mixed $value Value to convert
     * @return array DynamoDB attribute format
     */
    private function convertSimpleValue($value): array
    {
        if (is_string($value)) {
            return ['S' => $value];
        } elseif (is_int($value) || is_float($value)) {
            return ['N' => (string)$value];
        } elseif (is_bool($value)) {
            return ['BOOL' => $value];
        } elseif (is_null($value)) {
            return ['NULL' => true];
        } elseif (is_array($value)) {
            if (empty($value)) {
                return ['L' => []];
            }
            // Simple list conversion
            return ['L' => array_map([$this, 'convertSimpleValue'], $value)];
        }
        
        throw new DaxException('Unsupported attribute value type: ' . gettype($value));
    }

    /**
     * Encode a request for transmission
     *
     * @param int $methodId Method ID
     * @param array $request Request parameters
     * @return string Encoded request
     */
    private function encodeRequest(int $methodId, array $request): string
    {
        // This is a simplified encoding
        // In a full implementation, this would use CBOR encoding
        $payload = json_encode($request);
        
        // Simple protocol: [method_id:4][length:4][payload]
        return pack('NN', $methodId, strlen($payload)) . $payload;
    }

    /**
     * Receive a response from the connection
     *
     * @param DaxConnection $connection DAX connection
     * @return string Raw response data
     * @throws DaxException
     */
    private function receiveResponse(DaxConnection $connection): string
    {
        // Read response header (8 bytes: status + length)
        $header = $connection->receive(8);
        $headerData = unpack('Nstatus/Nlength', $header);
        
        if ($headerData['status'] !== 0) {
            throw new DaxException("DAX request failed with status: {$headerData['status']}");
        }
        
        // Read response payload
        if ($headerData['length'] > 0) {
            return $connection->receive($headerData['length']);
        }
        
        return '';
    }

    /**
     * Decode a response
     *
     * @param string $operation Operation name
     * @param string $response Raw response data
     * @return array Decoded response
     * @throws DaxException
     */
    private function decodeResponse(string $operation, string $response): array
    {
        if (empty($response)) {
            return [];
        }
        
        // This is a simplified decoding
        // In a full implementation, this would use CBOR decoding
        $decoded = json_decode($response, true);
        
        if ($decoded === null) {
            throw new DaxException('Failed to decode DAX response');
        }
        
        return $this->convertResponseAttributeValues($decoded);
    }

    /**
     * Convert response attribute values from DAX format
     *
     * @param array $response Response data
     * @return array Converted response
     */
    private function convertResponseAttributeValues(array $response): array
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
     * Convert from DynamoDB attribute format to simple value
     *
     * @param array $attribute DynamoDB attribute
     * @return mixed Simple value
     */
    private function convertFromDynamoDbAttribute(array $attribute)
    {
        $type = array_keys($attribute)[0];
        $value = $attribute[$type];
        
        switch ($type) {
            case 'S':
                return $value;
            case 'N':
                return is_float($value) ? (float)$value : (int)$value;
            case 'BOOL':
                return $value;
            case 'NULL':
                return null;
            case 'L':
                return array_map([$this, 'convertFromDynamoDbAttribute'], $value);
            case 'M':
                return $this->convertResponseAttributeValues($value);
            default:
                return $value;
        }
    }
}
