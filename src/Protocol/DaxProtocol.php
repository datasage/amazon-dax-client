<?php

declare(strict_types=1);

namespace Dax\Protocol;

use Dax\Connection\DaxConnection;
use Dax\Exception\DaxException;
use Dax\Cache\KeySchemaCache;
use Dax\Cache\AttributeListCache;
use Dax\Encoder\DaxEncoder;
use Dax\Decoder\DaxDecoder;

/**
 * Handles the DAX protocol communication
 */
class DaxProtocol
{
    private array $config;
    private ?KeySchemaCache $keySchemaCache;
    private ?AttributeListCache $attributeListCache;
    private DaxEncoder $encoder;
    private DaxDecoder $decoder;

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
        'DefineAttributeListId' => 657,
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
        $this->encoder = new DaxEncoder();
        $this->decoder = new DaxDecoder();
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
        $encodedRequest = $this->encoder->encodeRequest($methodId, $preparedRequest);

        // Send the request
        $connection->send($encodedRequest);

        // Receive and decode the response
        $response = $this->receiveResponse($connection);

        // Decode the response
        return $this->decoder->decodeResponse($operation, $response);
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
            return ['N' => (string) $value];
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


}
