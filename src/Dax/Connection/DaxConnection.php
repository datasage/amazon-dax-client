<?php

declare(strict_types=1);

namespace Dax\Connection;

use Dax\Exception\DaxException;

/**
 * Represents a single connection to a DAX node
 */
class DaxConnection
{
    private array $endpoint;
    private array $config;
    private $socket = null;
    private bool $connected = false;
    private int $lastActivity;
    private int $requestCount = 0;

    /**
     * Constructor
     *
     * @param array $endpoint Endpoint configuration
     * @param array $config Connection configuration
     */
    public function __construct(array $endpoint, array $config)
    {
        $this->endpoint = $endpoint;
        $this->config = $config;
        $this->lastActivity = time();
    }

    /**
     * Connect to the DAX node
     *
     * @throws DaxException
     */
    public function connect(): void
    {
        if ($this->connected) {
            return;
        }

        $host = $this->endpoint['host'];
        $port = $this->endpoint['port'];
        $ssl = $this->endpoint['ssl'] ?? false;

        $context = stream_context_create();

        if ($ssl) {
            stream_context_set_option($context, 'ssl', 'verify_peer', !$this->config['skip_hostname_verification']);
            stream_context_set_option($context, 'ssl', 'verify_peer_name', !$this->config['skip_hostname_verification']);
        }

        $protocol = $ssl ? 'ssl' : 'tcp';
        $address = "{$protocol}://{$host}:{$port}";

        $timeout = $this->config['connect_timeout'] / 1000; // Convert to seconds

        $this->socket = stream_socket_client(
            $address,
            $errno,
            $errstr,
            $timeout,
            STREAM_CLIENT_CONNECT,
            $context,
        );

        if ($this->socket === false) {
            throw new DaxException("Failed to connect to {$host}:{$port}: {$errstr} ({$errno})");
        }

        // Set socket options
        stream_set_timeout($this->socket, (int) ($this->config['request_timeout'] / 1000));
        stream_set_blocking($this->socket, true);

        $this->connected = true;
        $this->lastActivity = time();
    }

    /**
     * Close the connection
     */
    public function close(): void
    {
        if ($this->socket) {
            fclose($this->socket);
            $this->socket = null;
        }
        $this->connected = false;
    }

    /**
     * Check if the connection is active
     *
     * @return bool True if connected
     */
    public function isConnected(): bool
    {
        if (!$this->connected || !$this->socket) {
            return false;
        }

        // Check if socket is still valid
        $meta = stream_get_meta_data($this->socket);
        return !$meta['eof'] && !$meta['timed_out'];
    }

    /**
     * Check if the connection is idle
     *
     * @param int $idleTimeout Idle timeout in milliseconds
     * @return bool True if connection is idle
     */
    public function isIdle(int $idleTimeout): bool
    {
        $idleTime = (time() - $this->lastActivity) * 1000; // Convert to milliseconds
        return $idleTime > $idleTimeout;
    }

    /**
     * Send data to the DAX node
     *
     * @param string $data Data to send
     * @throws DaxException
     */
    public function send(string $data): void
    {
        if (!$this->isConnected()) {
            throw new DaxException('Connection is not active');
        }

        $written = fwrite($this->socket, $data);
        if ($written === false || $written !== strlen($data)) {
            throw new DaxException('Failed to send data to DAX node');
        }

        $this->lastActivity = time();
        $this->requestCount++;
    }

    /**
     * Receive data from the DAX node
     *
     * @param int $length Number of bytes to receive
     * @return string Received data
     * @throws DaxException
     */
    public function receive(int $length): string
    {
        if (!$this->isConnected()) {
            throw new DaxException('Connection is not active');
        }

        $data = '';
        $remaining = $length;

        while ($remaining > 0) {
            $chunk = fread($this->socket, $remaining);
            if ($chunk === false || $chunk === '') {
                $meta = stream_get_meta_data($this->socket);
                if ($meta['timed_out']) {
                    throw new DaxException('Timeout while receiving data from DAX node');
                }
                throw new DaxException('Failed to receive data from DAX node');
            }

            $data .= $chunk;
            $remaining -= strlen($chunk);
        }

        $this->lastActivity = time();
        return $data;
    }

    /**
     * Receive data until a specific delimiter is found
     *
     * @param string $delimiter Delimiter to look for
     * @param int $maxLength Maximum length to read
     * @return string Received data (without delimiter)
     * @throws DaxException
     */
    public function receiveUntil(string $delimiter, int $maxLength = 8192): string
    {
        if (!$this->isConnected()) {
            throw new DaxException('Connection is not active');
        }

        $data = '';
        $delimiterLength = strlen($delimiter);

        while (strlen($data) < $maxLength) {
            $char = fread($this->socket, 1);
            if ($char === false || $char === '') {
                $meta = stream_get_meta_data($this->socket);
                if ($meta['timed_out']) {
                    throw new DaxException('Timeout while receiving data from DAX node');
                }
                throw new DaxException('Failed to receive data from DAX node');
            }

            $data .= $char;

            // Check if we've found the delimiter
            if (strlen($data) >= $delimiterLength &&
                substr($data, -$delimiterLength) === $delimiter) {
                $this->lastActivity = time();
                return substr($data, 0, -$delimiterLength);
            }
        }

        throw new DaxException('Maximum length exceeded while waiting for delimiter');
    }

    /**
     * Get connection statistics
     *
     * @return array Connection statistics
     */
    public function getStats(): array
    {
        return [
            'host' => $this->endpoint['host'],
            'port' => $this->endpoint['port'],
            'ssl' => $this->endpoint['ssl'] ?? false,
            'connected' => $this->connected,
            'last_activity' => $this->lastActivity,
            'request_count' => $this->requestCount,
            'idle_time' => (time() - $this->lastActivity) * 1000,
        ];
    }

    /**
     * Get the endpoint information
     *
     * @return array Endpoint configuration
     */
    public function getEndpoint(): array
    {
        return $this->endpoint;
    }

    /**
     * Destructor - ensure connection is closed
     */
    public function __destruct()
    {
        $this->close();
    }
}
