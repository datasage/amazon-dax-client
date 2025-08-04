<?php

declare(strict_types=1);

namespace Dax\Protocol;

use Dax\Auth\DaxAuthenticator;
use Dax\Cache\AttributeListCache;
use Dax\Cache\KeySchemaCache;
use Dax\Connection\DaxConnection;
use Dax\Decoder\DaxDecoder;
use Dax\Encoder\DaxEncoder;
use Dax\Exception\DaxException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

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
    private LoggerInterface $logger;
    private bool $debugLogging;
    private ?DaxAuthenticator $authenticator;
    private ?int $lastAuthTime = null;
    private const AUTH_EXPIRATION_SECONDS = 300; // 5 minutes

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
        'DescribeTable' => 4,
        'DefineKeySchema' => 681,
        'DefineAttributeList' => 656,
        'DefineAttributeListId' => 657,
    ];


    /**
     * Constructor
     *
     * @param array $config Protocol configuration
     * @param LoggerInterface|null $logger Logger instance
     * @param DaxAuthenticator|null $authenticator Authenticator instance
     */
    public function __construct(array $config, ?LoggerInterface $logger = null, ?DaxAuthenticator $authenticator = null)
    {
        $this->config = $config;
        $this->keySchemaCache = $config['key_schema_cache'] ?? null;
        $this->attributeListCache = $config['attribute_list_cache'] ?? null;
        $this->encoder = new DaxEncoder();
        $this->decoder = new DaxDecoder();
        $this->logger = $logger ?? new NullLogger();
        $this->debugLogging = $config['debug_logging'] ?? false;
        $this->authenticator = $authenticator;
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
        // Send authentication if authenticator is available and auth has expired or not been sent yet
        if ($this->authenticator && $this->isAuthExpired()) {
            $this->sendAuthentication($connection);
        }

        $methodId = self::METHOD_IDS[$operation] ?? null;
        if ($methodId === null) {
            throw new DaxException("Unsupported operation: {$operation}");
        }

        // Log the incoming request in diagnostic format (only if debug logging is enabled)
        if ($this->debugLogging) {
            $this->logger->debug('DAX Protocol Request', [
                'operation' => $operation,
                'method_id' => $methodId,
                'original_request' => $this->formatRequestForLogging($request),
                'connection' => spl_object_hash($connection),
            ]);
        }

        // Prepare the request
        $preparedRequest = $this->prepareRequest($operation, $request, $connection);

        // Log the prepared request (only if debug logging is enabled)
        if ($this->debugLogging) {
            $this->logger->debug('DAX Protocol Prepared Request', [
                'operation' => $operation,
                'prepared_request' => $this->formatRequestForLogging($preparedRequest),
            ]);
        }

        // Encode the request
        $encodedRequest = $this->encoder->encodeRequest($methodId, $preparedRequest);

        // Log encoded request details (only if debug logging is enabled)
        if ($this->debugLogging) {
            $this->logger->debug('DAX Protocol Encoded Request', [
                'operation' => $operation,
                'encoded_size_bytes' => strlen($encodedRequest),
                'encoded_hex_preview' => substr(bin2hex($encodedRequest), 0, 100) . (strlen($encodedRequest) > 50 ? '...' : ''),
            ]);
        }

        // Send the request
        $connection->send($encodedRequest);

        // Receive and decode the response
        $response = $this->receiveResponse($connection);

        // Log raw response details (only if debug logging is enabled)
        if ($this->debugLogging) {
            $this->logger->debug('DAX Protocol Raw Response', [
                'operation' => $operation,
                'response_size_bytes' => strlen($response),
                'response_hex_preview' => substr(bin2hex($response), 0, 100) . (strlen($response) > 50 ? '...' : ''),
            ]);
        }

        // Decode the response
        $decodedResponse = $this->decoder->decodeResponse($operation, $response);

        // Log the decoded response in diagnostic format (only if debug logging is enabled)
        if ($this->debugLogging) {
            $this->logger->debug('DAX Protocol Decoded Response', [
                'operation' => $operation,
                'decoded_response' => $this->formatResponseForLogging($decodedResponse),
            ]);
        }

        return $decodedResponse;
    }

    /**
     * Format request data for diagnostic logging
     *
     * @param array $request Request data
     * @return array Formatted request for logging
     */
    private function formatRequestForLogging(array $request): array
    {
        $formatted = [];

        foreach ($request as $key => $value) {
            if (is_array($value)) {
                $formatted[$key] = $this->formatArrayForLogging($value);
            } else {
                $formatted[$key] = $value;
            }
        }

        return $formatted;
    }

    /**
     * Format response data for diagnostic logging
     *
     * @param array $response Response data
     * @return array Formatted response for logging
     */
    private function formatResponseForLogging(array $response): array
    {
        $formatted = [];

        foreach ($response as $key => $value) {
            if (is_array($value)) {
                $formatted[$key] = $this->formatArrayForLogging($value);
            } else {
                $formatted[$key] = $value;
            }
        }

        return $formatted;
    }

    /**
     * Format array data for diagnostic logging with size limits
     *
     * @param array $data Array data to format
     * @param int $maxDepth Maximum recursion depth
     * @param int $currentDepth Current recursion depth
     * @return array|string Formatted array or truncation message
     */
    private function formatArrayForLogging(array $data, int $maxDepth = 3, int $currentDepth = 0): array|string
    {
        if ($currentDepth >= $maxDepth) {
            return '[TRUNCATED: Max depth reached]';
        }

        if (count($data) > 50) {
            return '[TRUNCATED: Array too large (' . count($data) . ' items)]';
        }

        $formatted = [];

        foreach ($data as $key => $value) {
            if (is_array($value)) {
                $formatted[$key] = $this->formatArrayForLogging($value, $maxDepth, $currentDepth + 1);
            } elseif (is_string($value) && strlen($value) > 200) {
                $formatted[$key] = substr($value, 0, 200) . '... [TRUNCATED: String too long]';
            } else {
                $formatted[$key] = $value;
            }
        }

        return $formatted;
    }

    /**
     * Prepare a request for encoding
     *
     * @param string $operation Operation name
     * @param array $request Request parameters
     * @param DaxConnection|null $connection DAX connection for DescribeTable calls
     * @return array Prepared request
     * @throws DaxException
     */
    private function prepareRequest(string $operation, array $request, ?DaxConnection $connection = null): array
    {
        switch ($operation) {
            case 'GetItem':
            case 'PutItem':
            case 'DeleteItem':
            case 'UpdateItem':
                return $this->prepareSingleItemRequest($request, $connection);

            case 'BatchGetItem':
                return $this->prepareBatchGetRequest($request, $connection);

            case 'BatchWriteItem':
                return $this->prepareBatchWriteRequest($request, $connection);

            case 'Query':
            case 'Scan':
                return $this->prepareQueryScanRequest($request);

            case 'DescribeTable':
                // DescribeTable doesn't need key validation, just return as-is
                return $request;

            default:
                return $request;
        }
    }

    /**
     * Prepare a single item request
     *
     * @param array $request Request parameters
     * @param DaxConnection|null $connection DAX connection for DescribeTable calls
     * @return array Prepared request
     */
    private function prepareSingleItemRequest(array $request, ?DaxConnection $connection = null): array
    {
        // Ensure table name is present
        if (!isset($request['TableName'])) {
            throw new DaxException('TableName is required');
        }

        // Get and validate key schema if available
        $keySchema = $this->getKeySchema($request['TableName'], $connection);

        // Convert attribute values to DAX format
        if (isset($request['Key'])) {
            $request['Key'] = $this->convertAttributeValues($request['Key']);

            // Validate key against schema if available
            if ($keySchema) {
                $this->validateKey($request['Key'], $keySchema, $request['TableName']);
            }
        }

        if (isset($request['Item'])) {
            $request['Item'] = $this->convertAttributeValues($request['Item']);

            // Validate item key against schema if available
            if ($keySchema) {
                $itemKey = $this->extractKeyFromItem($request['Item'], $keySchema);
                if ($itemKey) {
                    $this->validateKey($itemKey, $keySchema, $request['TableName']);
                }
            }
        }

        return $request;
    }

    /**
     * Prepare a batch get request
     *
     * @param array $request Request parameters
     * @param DaxConnection|null $connection DAX connection for DescribeTable calls
     * @return array Prepared request
     */
    private function prepareBatchGetRequest(array $request, ?DaxConnection $connection = null): array
    {
        if (!isset($request['RequestItems'])) {
            throw new DaxException('RequestItems is required');
        }

        foreach ($request['RequestItems'] as $tableName => &$tableRequest) {
            if (isset($tableRequest['Keys'])) {
                // Get key schema for validation
                $keySchema = $this->getKeySchema($tableName, $connection);

                foreach ($tableRequest['Keys'] as &$key) {
                    $key = $this->convertAttributeValues($key);

                    // Validate key against schema if available
                    if ($keySchema) {
                        $this->validateKey($key, $keySchema, $tableName);
                    }
                }
            }
        }

        return $request;
    }

    /**
     * Prepare a batch write request
     *
     * @param array $request Request parameters
     * @param DaxConnection|null $connection DAX connection for DescribeTable calls
     * @return array Prepared request
     */
    private function prepareBatchWriteRequest(array $request, ?DaxConnection $connection = null): array
    {
        if (!isset($request['RequestItems'])) {
            throw new DaxException('RequestItems is required');
        }

        foreach ($request['RequestItems'] as $tableName => &$writeRequests) {
            // Get key schema for validation
            $keySchema = $this->getKeySchema($tableName, $connection);

            foreach ($writeRequests as &$writeRequest) {
                if (isset($writeRequest['PutRequest']['Item'])) {
                    $writeRequest['PutRequest']['Item'] = $this->convertAttributeValues($writeRequest['PutRequest']['Item']);

                    // Validate item key against schema if available
                    if ($keySchema) {
                        $itemKey = $this->extractKeyFromItem($writeRequest['PutRequest']['Item'], $keySchema);
                        if ($itemKey) {
                            $this->validateKey($itemKey, $keySchema, $tableName);
                        }
                    }
                }
                if (isset($writeRequest['DeleteRequest']['Key'])) {
                    $writeRequest['DeleteRequest']['Key'] = $this->convertAttributeValues($writeRequest['DeleteRequest']['Key']);

                    // Validate key against schema if available
                    if ($keySchema) {
                        $this->validateKey($writeRequest['DeleteRequest']['Key'], $keySchema, $tableName);
                    }
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
     * Check if authentication has expired or hasn't been sent yet
     *
     * @return bool True if auth has expired or not been sent, false otherwise
     */
    private function isAuthExpired(): bool
    {
        // If auth has never been sent, it's considered expired
        if ($this->lastAuthTime === null) {
            return true;
        }

        // Check if auth has expired (5 minutes)
        $currentTime = time();
        $timeSinceAuth = $currentTime - $this->lastAuthTime;

        return $timeSinceAuth >= self::AUTH_EXPIRATION_SECONDS;
    }

    /**
     * Send authentication request using CBOR encoding
     *
     * @param DaxConnection $connection DAX connection
     * @throws DaxException
     */
    private function sendAuthentication(DaxConnection $connection): void
    {
        try {
            // Generate signature information (always uses dax.amazonaws.com as canonical host)
            $signature = $this->authenticator->generateSignature();

            if ($this->debugLogging) {
                $this->logger->debug('DAX Protocol Authentication', $signature);
            }

            // Method ID for authorizeConnection is 1489122155
            $authRequest = $this->encoder->encodeAuthRequest(
                1489122155, // authorizeConnection method ID
                $signature['access_key'],
                $signature['signature'],
                $signature['string_to_sign'],
                $signature['token'],
                DaxConnection::USER_AGENT, // user agent
            );

            $connection->send($authRequest);

            // Record the timestamp of successful authentication
            $this->lastAuthTime = time();

        } catch (\Exception $e) {
            throw new DaxException('Failed to send authentication: ' . $e->getMessage(), 0, $e);
        }
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
        // The Go implementation uses pure CBOR format:
        // [CBOR_ERROR_ARRAY][CBOR_RESPONSE_DATA]
        // We need to read the entire CBOR stream and handle errors within the decoder

        // Read initial bytes to determine response size
        // CBOR responses are variable length, so we need to read incrementally
        $buffer = '';
        $chunkSize = 1024; // Read in 1KB chunks

        do {
            $chunk = $connection->receive($chunkSize);
            if ($chunk === '') {
                break; // No more data
            }
            $buffer .= $chunk;
        } while (strlen($chunk) === $chunkSize);

        return $buffer;
    }

    /**
     * Get key schema for a table from cache or retrieve it
     *
     * @param string $tableName Table name
     * @param DaxConnection|null $connection DAX connection for DescribeTable calls
     * @return array|null Key schema or null if not available
     */
    private function getKeySchema(string $tableName, ?DaxConnection $connection = null): ?array
    {
        if (!$this->keySchemaCache) {
            return null;
        }

        // Try to get from cache first
        $keySchema = $this->keySchemaCache->get($tableName);
        if ($keySchema !== null) {
            return $keySchema;
        }

        // If not in cache and we have a connection, try to retrieve it via DescribeTable
        if ($connection !== null) {
            try {
                $describeRequest = ['TableName' => $tableName];
                $response = $this->executeRequest($connection, 'DescribeTable', $describeRequest);

                // Extract key schema from DescribeTable response
                if (isset($response['Table']['KeySchema'])) {
                    $keySchema = $this->extractKeySchemaFromDescribeTable($response['Table']['KeySchema']);

                    // Cache the key schema for future use
                    $this->cacheKeySchema($tableName, $keySchema);

                    return $keySchema;
                }
            } catch (\Exception $e) {
                // Log the error but don't fail the operation
                $this->logger->warning("Failed to retrieve key schema for table '{$tableName}': " . $e->getMessage());
            }
        }

        // Return null if we couldn't retrieve the key schema
        return null;
    }

    /**
     * Extract key schema from DescribeTable response
     *
     * @param array $keySchemaResponse KeySchema from DescribeTable response
     * @return array Internal key schema format
     */
    private function extractKeySchemaFromDescribeTable(array $keySchemaResponse): array
    {
        $keySchema = [];

        foreach ($keySchemaResponse as $keyElement) {
            if ($keyElement['KeyType'] === 'HASH') {
                $keySchema['HashKeyElement'] = [
                    'AttributeName' => $keyElement['AttributeName'],
                    'AttributeType' => 'S', // Default to string, could be enhanced to get actual type
                ];
            } elseif ($keyElement['KeyType'] === 'RANGE') {
                $keySchema['RangeKeyElement'] = [
                    'AttributeName' => $keyElement['AttributeName'],
                    'AttributeType' => 'S', // Default to string, could be enhanced to get actual type
                ];
            }
        }

        return $keySchema;
    }

    /**
     * Validate a key against the table's key schema
     *
     * @param array $key Key to validate
     * @param array $keySchema Key schema
     * @param string $tableName Table name for error messages
     * @throws DaxException If key is invalid
     */
    private function validateKey(array $key, array $keySchema, string $tableName): void
    {
        // Validate hash key
        if (isset($keySchema['HashKeyElement'])) {
            $hashKeyName = $keySchema['HashKeyElement']['AttributeName'];
            if (!isset($key[$hashKeyName])) {
                throw new DaxException("Missing hash key '{$hashKeyName}' for table '{$tableName}'");
            }
        }

        // Validate range key if present in schema
        if (isset($keySchema['RangeKeyElement'])) {
            $rangeKeyName = $keySchema['RangeKeyElement']['AttributeName'];
            if (!isset($key[$rangeKeyName])) {
                throw new DaxException("Missing range key '{$rangeKeyName}' for table '{$tableName}'");
            }
        }

        // Validate that no extra attributes are present in the key
        $allowedKeys = [];
        if (isset($keySchema['HashKeyElement'])) {
            $allowedKeys[] = $keySchema['HashKeyElement']['AttributeName'];
        }
        if (isset($keySchema['RangeKeyElement'])) {
            $allowedKeys[] = $keySchema['RangeKeyElement']['AttributeName'];
        }

        foreach (array_keys($key) as $keyName) {
            if (!in_array($keyName, $allowedKeys)) {
                throw new DaxException("Invalid key attribute '{$keyName}' for table '{$tableName}'. Only key attributes are allowed.");
            }
        }
    }

    /**
     * Extract key attributes from an item based on key schema
     *
     * @param array $item Item data
     * @param array $keySchema Key schema
     * @return array|null Key attributes or null if key cannot be extracted
     */
    private function extractKeyFromItem(array $item, array $keySchema): ?array
    {
        $key = [];

        // Extract hash key
        if (isset($keySchema['HashKeyElement'])) {
            $hashKeyName = $keySchema['HashKeyElement']['AttributeName'];
            if (isset($item[$hashKeyName])) {
                $key[$hashKeyName] = $item[$hashKeyName];
            } else {
                return null; // Cannot extract key without hash key
            }
        }

        // Extract range key if present
        if (isset($keySchema['RangeKeyElement'])) {
            $rangeKeyName = $keySchema['RangeKeyElement']['AttributeName'];
            if (isset($item[$rangeKeyName])) {
                $key[$rangeKeyName] = $item[$rangeKeyName];
            } else {
                return null; // Cannot extract key without range key
            }
        }

        return empty($key) ? null : $key;
    }

    /**
     * Store key schema in cache
     *
     * @param string $tableName Table name
     * @param array $keySchema Key schema
     */
    public function cacheKeySchema(string $tableName, array $keySchema): void
    {
        if ($this->keySchemaCache) {
            $this->keySchemaCache->put($tableName, $keySchema);
        }
    }

    /**
     * Get cached key schema for a table
     *
     * @param string $tableName Table name
     * @return array|null Key schema or null if not cached
     */
    public function getCachedKeySchema(string $tableName): ?array
    {
        if (!$this->keySchemaCache) {
            return null;
        }

        return $this->keySchemaCache->get($tableName);
    }
}
