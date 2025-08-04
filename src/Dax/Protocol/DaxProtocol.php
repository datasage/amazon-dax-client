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
        $preparedRequest = $this->prepareRequest($operation, $request);

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

            // Following Python implementation: send CBOR-encoded authentication
            // Method ID for authorizeConnection is 1489122155
            $authRequest = $this->encoder->encodeAuthRequest(
                1489122155, // authorizeConnection method ID
                $signature['access_key'],
                $signature['signature'],
                $signature['string_to_sign'],
                $signature['token'],
                'DaxPHPClient-1.0', // user agent
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


}
