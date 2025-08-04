<?php

declare(strict_types=1);

namespace Dax;

use Aws\Credentials\CredentialsInterface;
use Dax\Client\DaxClientInterface;
use Dax\Connection\ClusterManager;
use Dax\Exception\DaxException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Amazon DAX Client - Main entry point for DAX operations
 */
class AmazonDaxClient implements DaxClientInterface
{
    private ClusterManager $clusterManager;
    private array $config;
    private LoggerInterface $logger;
    private bool $closed = false;

    /**
     * Constructor
     *
     * @param array $config Configuration options
     */
    public function __construct(array $config = [])
    {
        $this->config = array_merge([
            'region' => 'us-east-1',
            'version' => 'latest',
            'endpoint_url' => null,
            'endpoints' => [],
            'credentials' => null,
            'connect_timeout' => 1000,
            'request_timeout' => 60000,
            'max_pending_connections_per_host' => 10,
            'max_concurrent_requests_per_connection' => 1000,
            'idle_timeout' => 30000,
            'skip_hostname_verification' => false,
            'logger' => null,
            'debug_logging' => false,
        ], $config);

        // Initialize logger - use provided logger or NullLogger as default
        $this->logger = $this->config['logger'] instanceof LoggerInterface
            ? $this->config['logger']
            : new NullLogger();

        $this->validateConfig();
        $this->clusterManager = new ClusterManager($this->config, $this->logger);
    }

    /**
     * Create a DAX client instance
     *
     * @param array $config Configuration options
     * @return self
     */
    public static function factory(array $config = []): self
    {
        return new self($config);
    }

    /**
     * {@inheritdoc}
     */
    public function batchGetItem(array $requestItems): array
    {
        $this->ensureNotClosed();

        $request = [
            'RequestItems' => $requestItems,
        ];

        return $this->executeRequest('BatchGetItem', $request);
    }

    /**
     * {@inheritdoc}
     */
    public function batchWriteItem(array $requestItems): array
    {
        $this->ensureNotClosed();

        $request = [
            'RequestItems' => $requestItems,
        ];

        return $this->executeRequest('BatchWriteItem', $request);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteItem(string $tableName, array $key, array $options = []): array
    {
        $this->ensureNotClosed();

        $request = array_merge([
            'TableName' => $tableName,
            'Key' => $key,
        ], $options);

        return $this->executeRequest('DeleteItem', $request);
    }

    /**
     * {@inheritdoc}
     */
    public function getItem(string $tableName, array $key, array $options = []): array
    {
        $this->ensureNotClosed();

        $request = array_merge([
            'TableName' => $tableName,
            'Key' => $key,
        ], $options);

        return $this->executeRequest('GetItem', $request);
    }

    /**
     * {@inheritdoc}
     */
    public function putItem(string $tableName, array $item, array $options = []): array
    {
        $this->ensureNotClosed();

        $request = array_merge([
            'TableName' => $tableName,
            'Item' => $item,
        ], $options);

        return $this->executeRequest('PutItem', $request);
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $tableName, array $options = []): array
    {
        $this->ensureNotClosed();

        $request = array_merge([
            'TableName' => $tableName,
        ], $options);

        return $this->executeRequest('Query', $request);
    }

    /**
     * {@inheritdoc}
     */
    public function scan(string $tableName, array $options = []): array
    {
        $this->ensureNotClosed();

        $request = array_merge([
            'TableName' => $tableName,
        ], $options);

        return $this->executeRequest('Scan', $request);
    }

    /**
     * {@inheritdoc}
     */
    public function updateItem(string $tableName, array $key, array $options = []): array
    {
        $this->ensureNotClosed();

        $request = array_merge([
            'TableName' => $tableName,
            'Key' => $key,
        ], $options);

        return $this->executeRequest('UpdateItem', $request);
    }

    /**
     * {@inheritdoc}
     */
    public function close(): void
    {
        if (!$this->closed) {
            $this->clusterManager->close();
            $this->closed = true;
        }
    }

    /**
     * Magic method for with-statement support
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * Execute a request through the cluster manager
     *
     * @param string $operation Operation name
     * @param array $request Request parameters
     * @return array Response
     * @throws DaxException
     */
    private function executeRequest(string $operation, array $request): array
    {
        try {
            return $this->clusterManager->executeRequest($operation, $request);
        } catch (\Exception $e) {
            throw new DaxException("Failed to execute {$operation}: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Validate configuration parameters
     *
     * @throws DaxException
     */
    private function validateConfig(): void
    {
        if (empty($this->config['endpoint_url']) && empty($this->config['endpoints'])) {
            throw new DaxException('Either endpoint_url or endpoints must be provided');
        }

        if (!empty($this->config['endpoint_url']) && !empty($this->config['endpoints'])) {
            throw new DaxException('Cannot specify both endpoint_url and endpoints');
        }

        // Require credentials to be provided and be a valid CredentialsInterface instance
        if (!isset($this->config['credentials']) || 
            $this->config['credentials'] === null || 
            $this->config['credentials'] === '') {
            throw new DaxException('Credentials must be provided');
        }

        if (!($this->config['credentials'] instanceof CredentialsInterface)) {
            throw new DaxException('Credentials must be an instance of Aws\Credentials\CredentialsInterface');
        }
    }

    /**
     * Ensure the client is not closed
     *
     * @throws DaxException
     */
    private function ensureNotClosed(): void
    {
        if ($this->closed) {
            throw new DaxException('Client has been closed');
        }
    }
}
