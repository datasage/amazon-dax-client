<?php

declare(strict_types=1);

namespace Dax\Connection;

use Dax\Exception\DaxException;

/**
 * Manages a pool of connections to DAX cluster nodes
 */
class ConnectionPool
{
    private array $config;
    private array $endpoints = [];
    private array $connections = [];
    private array $badConnections = [];
    private bool $closed = false;

    /**
     * Constructor
     *
     * @param array $config Configuration options
     */
    public function __construct(array $config)
    {
        $this->config = array_merge([
            'connect_timeout' => 1000,
            'request_timeout' => 60000,
            'max_pending_connections_per_host' => 10,
            'max_concurrent_requests_per_connection' => 1000,
            'idle_timeout' => 30000,
            'skip_hostname_verification' => false,
        ], $config);
    }

    /**
     * Add an endpoint to the pool
     *
     * @param array $endpoint Endpoint configuration
     */
    public function addEndpoint(array $endpoint): void
    {
        $key = $this->getEndpointKey($endpoint);
        $this->endpoints[$key] = $endpoint;
    }

    /**
     * Get a connection from the pool
     *
     * @return DaxConnection
     * @throws DaxException
     */
    public function getConnection(): DaxConnection
    {
        if ($this->closed) {
            throw new DaxException('Connection pool has been closed');
        }

        // Try to get an existing healthy connection
        foreach ($this->connections as $key => $connectionList) {
            foreach ($connectionList as $index => $connection) {
                if ($this->isConnectionHealthy($connection)) {
                    return $connection;
                }
                // Remove unhealthy connection
                unset($this->connections[$key][$index]);
            }
        }

        // Create a new connection
        return $this->createConnection();
    }

    /**
     * Mark a connection as bad
     *
     * @param DaxConnection $connection Connection to mark as bad
     */
    public function markConnectionBad(DaxConnection $connection): void
    {
        $connectionId = spl_object_hash($connection);
        $this->badConnections[$connectionId] = time();

        // Remove from active connections
        foreach ($this->connections as $key => &$connectionList) {
            foreach ($connectionList as $index => $conn) {
                if (spl_object_hash($conn) === $connectionId) {
                    unset($connectionList[$index]);
                    break 2;
                }
            }
        }
    }

    /**
     * Close all connections and clean up
     */
    public function close(): void
    {
        if (!$this->closed) {
            foreach ($this->connections as $connectionList) {
                foreach ($connectionList as $connection) {
                    $connection->close();
                }
            }
            $this->connections = [];
            $this->badConnections = [];
            $this->closed = true;
        }
    }

    /**
     * Create a new connection
     *
     * @return DaxConnection
     * @throws DaxException
     */
    private function createConnection(): DaxConnection
    {
        if (empty($this->endpoints)) {
            throw new DaxException('No endpoints available');
        }

        // Select an endpoint (simple round-robin for now)
        $endpoint = $this->selectEndpoint();
        $key = $this->getEndpointKey($endpoint);

        // Check if we've reached the connection limit for this endpoint
        $currentConnections = count($this->connections[$key] ?? []);
        if ($currentConnections >= $this->config['max_pending_connections_per_host']) {
            throw new DaxException("Maximum connections reached for endpoint: {$endpoint['host']}:{$endpoint['port']}");
        }

        try {
            $connection = new DaxConnection($endpoint, $this->config);
            $connection->connect();

            // Add to connection pool
            if (!isset($this->connections[$key])) {
                $this->connections[$key] = [];
            }
            $this->connections[$key][] = $connection;

            return $connection;
        } catch (\Exception $e) {
            throw new DaxException("Failed to create connection to {$endpoint['host']}:{$endpoint['port']}: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Select an endpoint for new connection
     *
     * @return array Selected endpoint
     */
    private function selectEndpoint(): array
    {
        // Simple round-robin selection
        static $lastIndex = -1;
        $endpoints = array_values($this->endpoints);
        $lastIndex = ($lastIndex + 1) % count($endpoints);
        return $endpoints[$lastIndex];
    }

    /**
     * Check if a connection is healthy
     *
     * @param DaxConnection $connection Connection to check
     * @return bool True if connection is healthy
     */
    private function isConnectionHealthy(DaxConnection $connection): bool
    {
        $connectionId = spl_object_hash($connection);

        // Check if marked as bad recently
        if (isset($this->badConnections[$connectionId])) {
            $badTime = $this->badConnections[$connectionId];
            // Remove from bad list after 30 seconds
            if (time() - $badTime > 30) {
                unset($this->badConnections[$connectionId]);
            } else {
                return false;
            }
        }

        return $connection->isConnected() && !$connection->isIdle($this->config['idle_timeout']);
    }

    /**
     * Get a unique key for an endpoint
     *
     * @param array $endpoint Endpoint configuration
     * @return string Unique key
     */
    private function getEndpointKey(array $endpoint): string
    {
        return "{$endpoint['host']}:{$endpoint['port']}";
    }
}
