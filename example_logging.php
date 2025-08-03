<?php

require_once __DIR__ . '/vendor/autoload.php';

use Amazon\Dax\AmazonDaxClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Create a logger instance
$logger = new Logger('dax-client');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Create DAX client with logger
$config = [
    'region' => 'us-east-1',
    'endpoints' => ['dax://test-cluster.abc123.dax-clusters.us-east-1.amazonaws.com:8111'],
    'logger' => $logger
];

echo "Creating DAX client with logging enabled...\n";

try {
    $client = new AmazonDaxClient($config);
    echo "DAX client created successfully!\n";
    
    // Close the client
    $client->close();
    echo "DAX client closed.\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
}

echo "\n--- Example without logger (uses NullLogger) ---\n";

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
