<?php

require_once __DIR__ . '/vendor/autoload.php';

use Dax\AmazonDaxClient;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

// Create a logger instance with detailed formatting for debug output
$logger = new Logger('dax-client');
$handler = new StreamHandler('php://stdout', Logger::DEBUG);

// Custom formatter for better diagnostic output
$formatter = new LineFormatter(
    "[%datetime%] %channel%.%level_name%: %message% %context%\n",
    'Y-m-d H:i:s.u'
);
$handler->setFormatter($formatter);
$logger->pushHandler($handler);

echo "=== DAX Client Logging Examples ===\n\n";

// Example 1: Basic logging (without debug logging)
echo "1. Basic logging (cluster management only):\n";
$basicConfig = [
    'region' => 'us-east-1',
    'endpoints' => ['dax://test-cluster.abc123.dax-clusters.us-east-1.amazonaws.com:8111'],
    'logger' => $logger
];

echo "Creating DAX client with basic logging enabled...\n";

try {
    $client = new AmazonDaxClient($basicConfig);
    echo "DAX client created successfully!\n";
    
    // Close the client
    $client->close();
    echo "DAX client closed.\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
}

echo "\n2. Debug logging enabled (detailed request/response logging):\n";

// Example 2: Debug logging enabled
$debugConfig = [
    'region' => 'us-east-1',
    'endpoints' => ['dax://test-cluster.abc123.dax-clusters.us-east-1.amazonaws.com:8111'],
    'logger' => $logger,
    'debug_logging' => true  // Enable detailed protocol logging
];

echo "Creating DAX client with debug logging enabled...\n";

try {
    $debugClient = new AmazonDaxClient($debugConfig);
    echo "DAX client with debug logging created successfully!\n";
    
    // Attempt a simple operation to show debug logging
    echo "Attempting getItem operation to demonstrate debug logging...\n";
    try {
        $result = $debugClient->getItem('TestTable', ['id' => ['S' => 'test-key']]);
    } catch (Exception $e) {
        echo "Expected error (no real cluster): " . $e->getMessage() . "\n";
    }
    
    $debugClient->close();
    echo "Debug client closed.\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
}

echo "\n3. No logger (uses NullLogger, no output):\n";

$configWithoutLogger = [
    'region' => 'us-east-1',
    'endpoints' => ['dax://test-cluster.abc123.dax-clusters.us-east-1.amazonaws.com:8111']
];

try {
    $clientWithoutLogger = new AmazonDaxClient($configWithoutLogger);
    echo "DAX client created without logger (no log output expected).\n";
    $clientWithoutLogger->close();
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
}

echo "\n=== Logging Configuration Options ===\n";
echo "- 'logger': PSR-3 LoggerInterface instance (optional, defaults to NullLogger)\n";
echo "- 'debug_logging': boolean (optional, defaults to false)\n";
echo "  When true, enables detailed request/response logging including:\n";
echo "  * Original and prepared request data\n";
echo "  * Encoded request size and hex preview\n";
echo "  * Raw response size and hex preview\n";
echo "  * Decoded response data\n";
echo "\nNote: Debug logging may impact performance and should only be enabled for troubleshooting.\n";
