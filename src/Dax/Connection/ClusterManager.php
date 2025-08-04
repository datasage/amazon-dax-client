<?php

declare(strict_types=1);

namespace Dax\Connection;

use Dax\Auth\DaxAuthenticator;
use Dax\Cache\AttributeListCache;
use Dax\Cache\KeySchemaCache;
use Dax\Exception\DaxException;
use Dax\Protocol\DaxProtocol;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Manages DAX cluster connections and routing
 */
class ClusterManager
{
    private array $config;
    private LoggerInterface $logger;
    private array $endpoints = [];
    private ?ConnectionPool $connectionPool = null;
    private ?DaxProtocol $protocol = null;
    private ?KeySchemaCache $keySchemaCache = null;
    private ?AttributeListCache $attributeListCache = null;
    private ?DaxAuthenticator $authenticator = null;
    private bool $closed = false;

    /**
     * Constructor
     *
     * @param array $config Configuration options
     * @param LoggerInterface|null $logger Optional logger instance
     */
    public function __construct(array $config, ?LoggerInterface $logger = null)
    {
        $this->config = $config;
        $this->logger = $logger ?? new NullLogger();
        $this->initialize();
    }

    /**
     * Execute a request against the DAX cluster
     *
     * @param string $operation Operation name
     * @param array $request Request parameters
     * @return array Response
     * @throws DaxException
     */
    public function executeRequest(string $operation, array $request): array
    {
        if ($this->closed) {
            $this->logger->error('Attempted to execute request on closed ClusterManager', [
                'operation' => $operation,
            ]);
            throw new DaxException('ClusterManager has been closed');
        }

        $this->logger->debug('Executing DAX request', [
            'operation' => $operation,
            'request_keys' => array_keys($request),
        ]);

        $connection = $this->connectionPool->getConnection();

        try {
            $result = $this->protocol->executeRequest($connection, $operation, $request);
            $this->logger->debug('DAX request completed successfully', [
                'operation' => $operation,
            ]);
            return $result;
        } catch (\Exception $e) {
            $this->logger->warning('DAX request failed, marking connection as bad', [
                'operation' => $operation,
                'error' => $e->getMessage(),
                'connection' => spl_object_hash($connection),
            ]);
            // Mark connection as potentially bad and retry with a different one
            $this->connectionPool->markConnectionBad($connection);
            throw new DaxException("Request failed: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Close all connections and clean up resources
     */
    public function close(): void
    {
        if (!$this->closed) {
            $this->logger->info('Closing DAX cluster manager');
            if ($this->connectionPool) {
                $this->connectionPool->close();
            }
            $this->closed = true;
            $this->logger->debug('DAX cluster manager closed successfully');
        }
    }

    /**
     * Initialize the cluster manager
     *
     * @throws DaxException
     */
    private function initialize(): void
    {
        $this->logger->info('Initializing DAX cluster manager', [
            'endpoints_count' => count($this->config['endpoints'] ?? []),
            'region' => $this->config['region'] ?? 'unknown',
        ]);

        try {
            $this->parseEndpoints();
            $this->initializeCaches();
            $this->initializeAuthenticator();
            $this->initializeProtocol();
            $this->initializeConnectionPool();
            $this->discoverCluster();

            $this->logger->info('DAX cluster manager initialized successfully', [
                'endpoints_count' => count($this->endpoints),
            ]);
        } catch (\Exception $e) {
            $this->logger->error('Failed to initialize DAX cluster manager', [
                'error' => $e->getMessage(),
            ]);
            throw $e;
        }
    }

    /**
     * Parse endpoints from configuration
     *
     * @throws DaxException
     */
    private function parseEndpoints(): void
    {
        if (!empty($this->config['endpoint_url'])) {
            $this->endpoints = [$this->parseEndpoint($this->config['endpoint_url'])];
        } elseif (!empty($this->config['endpoints'])) {
            $this->endpoints = array_map([$this, 'parseEndpoint'], $this->config['endpoints']);
        } else {
            throw new DaxException('No endpoints configured');
        }
    }

    /**
     * Parse a single endpoint URL
     *
     * @param string $endpointUrl Endpoint URL
     * @return array Parsed endpoint information
     * @throws DaxException
     */
    private function parseEndpoint(string $endpointUrl): array
    {
        $parsed = parse_url($endpointUrl);

        if ($parsed === false) {
            throw new DaxException("Invalid endpoint URL: {$endpointUrl}");
        }

        $scheme = $parsed['scheme'] ?? '';
        $host = $parsed['host'] ?? '';
        $port = $parsed['port'] ?? null;

        if (!in_array($scheme, ['dax', 'daxs'])) {
            throw new DaxException("Unsupported scheme: {$scheme}. Use 'dax' or 'daxs'");
        }

        if (empty($host)) {
            throw new DaxException("Missing host in endpoint URL: {$endpointUrl}");
        }

        // Default ports
        if ($port === null) {
            $port = ($scheme === 'daxs') ? 9111 : 8111;
        }

        return [
            'scheme' => $scheme,
            'host' => $host,
            'port' => $port,
            'ssl' => $scheme === 'daxs',
        ];
    }

    /**
     * Initialize caches
     */
    private function initializeCaches(): void
    {
        $this->keySchemaCache = new KeySchemaCache(
            $this->config['key_cache_size'] ?? 1000,
            $this->config['key_cache_ttl'] ?? 60000,
        );

        $this->attributeListCache = new AttributeListCache(
            $this->config['attr_cache_size'] ?? 1000,
        );
    }

    /**
     * Initialize the authenticator
     */
    private function initializeAuthenticator(): void
    {
        // Only initialize authenticator if credentials are provided
        if (!empty($this->config['credentials'])) {
            $this->authenticator = new DaxAuthenticator($this->config);
            $this->logger->debug('DAX authenticator initialized');
        } else {
            $this->logger->debug('No credentials configured, skipping authenticator initialization');
        }
    }

    /**
     * Initialize the DAX protocol handler
     */
    private function initializeProtocol(): void
    {
        $this->protocol = new DaxProtocol([
            'region' => $this->config['region'],
            'credentials' => $this->config['credentials'] ?? null,
            'key_schema_cache' => $this->keySchemaCache,
            'attribute_list_cache' => $this->attributeListCache,
            'debug_logging' => $this->config['debug_logging'] ?? false,
        ], $this->logger, $this->authenticator);
    }

    /**
     * Initialize the connection pool
     */
    private function initializeConnectionPool(): void
    {
        $this->connectionPool = new ConnectionPool([
            'endpoints' => $this->endpoints,
            'connect_timeout' => $this->config['connect_timeout'] ?? 5000,
            'request_timeout' => $this->config['request_timeout'] ?? 60000,
            'max_pending_connections_per_host' => $this->config['max_pending_connections_per_host'] ?? 10,
            'max_concurrent_requests_per_connection' => $this->config['max_concurrent_requests_per_connection'] ?? 5,
            'idle_timeout' => $this->config['idle_timeout'] ?? 30000,
            'skip_hostname_verification' => $this->config['skip_hostname_verification'] ?? false,
        ]);
    }

    /**
     * Discover cluster nodes
     *
     * @throws DaxException
     */
    private function discoverCluster(): void
    {
        // For now, use the configured endpoints
        // In a full implementation, this would query the cluster discovery endpoint
        // to get the actual cluster node endpoints

        foreach ($this->endpoints as $endpoint) {
            $this->connectionPool->addEndpoint($endpoint);
        }
    }
}
