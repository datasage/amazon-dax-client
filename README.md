# Amazon DAX Client for PHP

A PHP implementation of the Amazon DynamoDB Accelerator (DAX) client, providing high-performance caching for DynamoDB operations.

This is currently in development and should not be used. It will be ready for use once a release is published.

## Recent Improvements

This implementation has undergone significant improvements and fixes:

### Security & Authentication Enhancements
- **Host Restriction**: All AWS SigV4 authentication signatures now use the canonical `dax.amazonaws.com` endpoint for enhanced security
- **Authentication Fixes**: Resolved host header formatting issues, hardcoded endpoint problems, and improved SigV4 compliance
- **Canonical Request Building**: Enhanced canonical request building with proper header handling and alphabetical sorting

### Protocol & Compatibility Fixes
- **CBOR Tag Correction**: Fixed CBOR tag values to match AWS official specification (3321-3324 instead of 258-261)
- **DAX-Specific CBOR Support**: Full implementation of DAX-specific CBOR encoding/decoding with support for DynamoDB sets (SS, NS, BS)
- **Protocol Compliance**: Improved compatibility with official AWS DAX protocol specification

### Implementation Completeness
- **Full DynamoDB Operations**: Support for all DynamoDB operations including batch operations, queries, and scans
- **Advanced Caching**: Key schema caching with TTL and attribute list caching with LRU eviction
- **Connection Management**: Robust connection pooling, health checking, and cluster discovery
- **Comprehensive Testing**: 26+ unit tests with full coverage of core functionality

## Requirements

- PHP 8.1 or newer
- ext-json
- ext-sockets
- ext-openssl
- aws/aws-sdk-php ^3.0

## Installation

```bash
composer require datasage/amazon-dax-client
```

## Basic Usage

### Creating a Client

```php
use Amazon\Dax\AmazonDaxClient;

// Using endpoint URL
$client = new AmazonDaxClient([
    'endpoint_url' => 'dax://your-cluster.abc123.dax-clusters.us-east-1.amazonaws.com',
    'region' => 'us-east-1'
]);

// Using multiple endpoints
$client = new AmazonDaxClient([
    'endpoints' => [
        'dax://node1.abc123.dax-clusters.us-east-1.amazonaws.com',
        'dax://node2.abc123.dax-clusters.us-east-1.amazonaws.com'
    ],
    'region' => 'us-east-1'
]);

// Using factory method
$client = AmazonDaxClient::factory([
    'endpoint_url' => 'dax://your-cluster.abc123.dax-clusters.us-east-1.amazonaws.com',
    'region' => 'us-east-1'
]);
```

### Basic Operations

#### Get Item

```php
$response = $client->getItem('MyTable', [
    'id' => ['S' => 'item-123']
]);

if (isset($response['Item'])) {
    echo "Found item: " . json_encode($response['Item']);
}
```

#### Put Item

```php
$response = $client->putItem('MyTable', [
    'id' => ['S' => 'item-123'],
    'name' => ['S' => 'John Doe'],
    'age' => ['N' => '30']
]);
```

#### Update Item

```php
$response = $client->updateItem('MyTable', 
    ['id' => ['S' => 'item-123']], 
    [
        'UpdateExpression' => 'SET #name = :name',
        'ExpressionAttributeNames' => ['#name' => 'name'],
        'ExpressionAttributeValues' => [':name' => ['S' => 'Jane Doe']]
    ]
);
```

#### Delete Item

```php
$response = $client->deleteItem('MyTable', [
    'id' => ['S' => 'item-123']
]);
```

#### Query

```php
$response = $client->query('MyTable', [
    'KeyConditionExpression' => 'id = :id',
    'ExpressionAttributeValues' => [
        ':id' => ['S' => 'item-123']
    ]
]);

foreach ($response['Items'] as $item) {
    echo "Item: " . json_encode($item) . "\n";
}
```

#### Scan

```php
$response = $client->scan('MyTable', [
    'FilterExpression' => 'age > :age',
    'ExpressionAttributeValues' => [
        ':age' => ['N' => '25']
    ]
]);
```

### Batch Operations

#### Batch Get Item

```php
$response = $client->batchGetItem([
    'MyTable' => [
        'Keys' => [
            ['id' => ['S' => 'item-1']],
            ['id' => ['S' => 'item-2']],
            ['id' => ['S' => 'item-3']]
        ]
    ]
]);
```

#### Batch Write Item

```php
$response = $client->batchWriteItem([
    'MyTable' => [
        [
            'PutRequest' => [
                'Item' => [
                    'id' => ['S' => 'item-4'],
                    'name' => ['S' => 'Alice']
                ]
            ]
        ],
        [
            'DeleteRequest' => [
                'Key' => ['id' => ['S' => 'item-5']]
            ]
        ]
    ]
]);
```

## Configuration Options

```php
$client = new AmazonDaxClient([
    'endpoint_url' => 'dax://cluster.dax-clusters.us-east-1.amazonaws.com',
    'region' => 'us-east-1',
    'connect_timeout' => 1000,           // Connection timeout in milliseconds
    'request_timeout' => 60000,          // Request timeout in milliseconds
    'max_pending_connections_per_host' => 10,
    'max_concurrent_requests_per_connection' => 1000,
    'idle_timeout' => 30000,             // Idle timeout in milliseconds
    'skip_hostname_verification' => false, // For SSL connections
    'key_cache_size' => 1000,            // Key schema cache size
    'key_cache_ttl' => 60000,            // Key schema cache TTL in milliseconds
    'attr_cache_size' => 1000            // Attribute list cache size
]);
```

## SSL/TLS Support

For encrypted DAX clusters, use the `daxs://` scheme:

```php
$client = new AmazonDaxClient([
    'endpoint_url' => 'daxs://your-cluster.abc123.dax-clusters.us-east-1.amazonaws.com',
    'region' => 'us-east-1'
]);
```

## Error Handling

```php
use Amazon\Dax\Exception\DaxException;

try {
    $response = $client->getItem('MyTable', ['id' => ['S' => 'item-123']]);
} catch (DaxException $e) {
    echo "DAX Error: " . $e->getMessage();
    echo "Error Code: " . $e->getErrorCode();
    echo "Request ID: " . $e->getRequestId();
}
```

## Resource Management

Always close the client when done to free up connections:

```php
$client->close();

// Or use with automatic cleanup
$client = new AmazonDaxClient($config);
try {
    // Use client
} finally {
    $client->close();
}
```

## Caching

The DAX client includes built-in caching for:

- **Key Schemas**: Cached with TTL to avoid repeated schema lookups
- **Attribute Lists**: LRU cache for attribute name mappings

Cache statistics can be accessed through the internal cache objects (advanced usage).

## Differences from Python Client

This PHP implementation provides the same core functionality as the Python DAX client with some differences:

1. **Simplified Protocol**: Uses a simplified protocol implementation (full CBOR support would require additional dependencies)
2. **PHP-specific Features**: Leverages PHP 8.1+ features like typed properties and union types
3. **Error Handling**: Uses PHP exceptions with DAX-specific error information
4. **Configuration**: PHP-style associative arrays for configuration

## Development

### Running Tests

```bash
# Install dependencies
composer install

# Run unit tests
./vendor/bin/phpunit

# Run with coverage
./vendor/bin/phpunit --coverage-html coverage
```

### Code Quality

```bash
# Run PHPStan
./vendor/bin/phpstan analyse

# Run PHP CodeSniffer
./vendor/bin/phpcs
```

## Architecture

The client consists of several key components:

- **AmazonDaxClient**: Main client interface
- **ClusterManager**: Manages DAX cluster connections and discovery
- **ConnectionPool**: Handles connection pooling and health checking
- **DaxConnection**: Individual connection to DAX nodes
- **DaxProtocol**: Handles DAX protocol communication
- **Caches**: Key schema and attribute list caching

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## Support

For issues and questions:

1. Check the existing issues on GitHub
2. Create a new issue with detailed information
3. Include PHP version, DAX cluster configuration, and error messages
