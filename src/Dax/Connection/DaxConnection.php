<?php

declare(strict_types=1);

namespace Dax\Connection;

use CBOR\CBORObject;
use Dax\Encoder\DaxEncoder;
use Dax\Exception\DaxException;

/**
 * Represents a single connection to a DAX node
 */
class DaxConnection
{
    private const MAGIC = 'J7yne5G';
    public const USER_AGENT = 'DaxPHPClient-1.0';
    
    private array $endpoint;
    private array $config;
    private DaxEncoder $encoder;
    /** @var resource */
    private $socket = null;
    private bool $connected = false;
    private int $lastActivity;
    private int $requestCount = 0;
    private int $sessionId;

    /**
     * Constructor
     *
     * @param array $endpoint Endpoint configuration
     * @param array $config Connection configuration
     * @param DaxEncoder|null $encoder CBOR encoder (optional, will create new if not provided)
     */
    public function __construct(array $endpoint, array $config, ?DaxEncoder $encoder = null)
    {
        $this->endpoint = $endpoint;
        $this->config = $config;
        $this->encoder = $encoder ?? new DaxEncoder();
        $this->sessionId = time() * 1000 + random_int(0, 999); // Generate unique session ID
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
        
        // Send initial handshake packets as per Go implementation
        $this->sendInitialHandshake();
    }

    /**
     * Send initial handshake packets as per Go implementation
     * Sends: magic, layering, session, header, client mode
     *
     * @throws DaxException
     */
    private function sendInitialHandshake(): void
    {
        try {
            // 1. Send magic string "J7yne5G"
            $magicObject = $this->encoder->arrayToCborObject(self::MAGIC);
            $this->sendCborObject($magicObject);
            
            // 2. Send layering (0)
            $layeringObject = $this->encoder->arrayToCborObject(0);
            $this->sendCborObject($layeringObject);
            
            // 3. Send session ID as string
            $sessionObject = $this->encoder->arrayToCborObject((string) $this->sessionId);
            $this->sendCborObject($sessionObject);
            
            // 4. Send header map with UserAgent
            $headerMap = ['UserAgent' => self::USER_AGENT];
            $headerObject = $this->encoder->arrayToCborObject($headerMap);
            $this->sendCborObject($headerObject);
            
            // 5. Send client mode (0)
            $clientModeObject = $this->encoder->arrayToCborObject(0);
            $this->sendCborObject($clientModeObject);
            
        } catch (\Exception $e) {
            throw new DaxException('Failed to send initial handshake: ' . $e->getMessage(), 0, $e);
        }
    }
    
    /**
     * Send a CBOR object over the socket
     *
     * @param CBORObject $cborObject CBOR object to send
     * @throws DaxException
     */
    private function sendCborObject(CBORObject $cborObject): void
    {
        $data = $cborObject->__toString();
        $written = fwrite($this->socket, $data);
        if ($written === false || $written !== strlen($data)) {
            throw new DaxException('Failed to send CBOR data to DAX node');
        }
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
