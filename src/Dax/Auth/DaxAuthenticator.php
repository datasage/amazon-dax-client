<?php

declare(strict_types=1);

namespace Dax\Auth;

use Aws\Credentials\CredentialsInterface;
use Aws\Signature\SignatureV4;
use Dax\Exception\DaxException;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\RequestInterface;

/**
 * Handles AWS authentication for DAX requests
 */
class DaxAuthenticator
{
    private SignatureV4 $signer;
    private string $region;
    private ?CredentialsInterface $credentials = null;

    /**
     * Constructor
     *
     * @param array $config Configuration options
     */
    public function __construct(array $config)
    {
        $this->region = $config['region'] ?? 'us-east-1';
        
        // Require explicit credentials
        if (!isset($config['credentials']) || !($config['credentials'] instanceof CredentialsInterface)) {
            throw new DaxException('Credentials must be provided via config');
        }
        
        $this->credentials = $config['credentials'];
        
        // Initialize signature v4 signer for DAX service
        $this->signer = new SignatureV4('dax', $this->region);
    }

    /**
     * Get AWS credentials
     *
     * @return CredentialsInterface
     */
    public function getCredentials(): CredentialsInterface
    {
        return $this->credentials;
    }

    /**
     * Generate authentication headers for DAX request
     *
     * @param string $host DAX endpoint host
     * @param string $payload Request payload
     * @return array<string, string> Authentication headers
     * @throws DaxException
     */
    public function generateAuthHeaders(string $host, string $payload): array
    {
        try {
            $credentials = $this->getCredentials();
            
            // Create a PSR-7 request for signing
            $request = new Request(
                'POST',
                'https://' . $host . '/',
                [
                    'Host' => $host,
                    'Content-Type' => 'application/x-amz-cbor-1.1'
                ],
                $payload
            );

            // Sign the request
            $signedRequest = $this->signer->signRequest($request, $credentials);
            
            // Extract headers needed for DAX
            $headers = [];
            foreach ($signedRequest->getHeaders() as $name => $values) {
                $lowerName = strtolower($name);
                if (in_array($lowerName, ['authorization', 'x-amz-date', 'x-amz-security-token'])) {
                    $headers[$name] = is_array($values) ? $values[0] : $values;
                }
            }
            
            return $headers;
        } catch (\Exception $e) {
            throw new DaxException('Failed to generate authentication headers: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Generate signature information similar to Python implementation
     *
     * @param string $host DAX endpoint host
     * @param string $payload Request payload
     * @return array{access_key: string, signature: string, string_to_sign: string, token: string|null} Signature information
     * @throws DaxException
     */
    public function generateSignature(string $host, string $payload): array
    {
        try {
            $credentials = $this->getCredentials();
            
            // Create a PSR-7 request for signing
            $request = new Request(
                'POST',
                'https://dax.amazonaws.com',
                [
                    'Host' => 'https://dax.amazonaws.com',
                    'Content-Type' => 'application/x-amz-cbor-1.1'
                ],
                $payload
            );

            // Sign the request
            $signedRequest = $this->signer->signRequest($request, $credentials);
            
            // Extract the authorization header to get the signature
            $authHeader = $signedRequest->getHeaderLine('Authorization');
            $signature = '';
            if (preg_match('/Signature=([a-f0-9]+)/', $authHeader, $matches)) {
                $signature = $matches[1];
            }
            
            // Build string to sign (simplified version)
            $stringToSign = sprintf(
                "AWS4-HMAC-SHA256\n%s\n%s/%s/dax/aws4_request\n%s",
                $signedRequest->getHeaderLine('X-Amz-Date'),
                substr($signedRequest->getHeaderLine('X-Amz-Date'), 0, 8),
                $this->region,
                hash('sha256', $this->buildCanonicalRequest($request))
            );
            
            return [
                'access_key' => $credentials->getAccessKeyId(),
                'signature' => $signature,
                'string_to_sign' => $stringToSign,
                'token' => $credentials->getSecurityToken(),
                'host' => $host
            ];
        } catch (\Exception $e) {
            throw new DaxException('Failed to generate signature: ' . $e->getMessage(), 0, $e);
        }
    }
    
    /**
     * Build canonical request for signing
     *
     * @param RequestInterface $request
     * @return string
     */
    private function buildCanonicalRequest(RequestInterface $request): string
    {
        $method = $request->getMethod();
        $uri = '/';
        $query = '';
        $headers = "host:" . $request->getHeaderLine('Host') . "\n" .
                  "x-amz-date:" . $request->getHeaderLine('X-Amz-Date') . "\n";
        $signedHeaders = 'host;x-amz-date';
        $payload = (string) $request->getBody();
        $payloadHash = hash('sha256', $payload);
        
        return implode("\n", [$method, $uri, $query, $headers, $signedHeaders, $payloadHash]);
    }
}
