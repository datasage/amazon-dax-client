<?php

namespace Dax\Tests\Unit;

use Aws\Credentials\Credentials;
use Dax\AmazonDaxClient;
use Dax\Connection\ClusterManager;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class LoggingTest extends TestCase
{
    private TestHandler $testHandler;
    private Logger $logger;

    protected function setUp(): void
    {
        $this->testHandler = new TestHandler();
        $this->logger = new Logger('test');
        $this->logger->pushHandler($this->testHandler);
    }

    /**
     * Create mock credentials for testing
     */
    private function createMockCredentials(): Credentials
    {
        return new Credentials('test-access-key', 'test-secret-key', 'test-session-token');
    }

    public function testAmazonDaxClientAcceptsLogger(): void
    {
        $config = [
            'region' => 'us-east-1',
            'endpoints' => ['dax://test-cluster.abc123.dax-clusters.us-east-1.amazonaws.com:8111'],
            'credentials' => $this->createMockCredentials(),
            'logger' => $this->logger,
        ];

        // This should not throw an exception and should accept the logger
        $client = new AmazonDaxClient($config);

        // Verify that initialization logging occurred
        $this->assertTrue($this->testHandler->hasInfoRecords());
        $this->assertTrue($this->testHandler->hasRecord('Initializing DAX cluster manager', Level::Info));

        $client->close();
    }

    public function testAmazonDaxClientUsesNullLoggerByDefault(): void
    {
        $config = [
            'region' => 'us-east-1',
            'endpoints' => ['dax://test-cluster.abc123.dax-clusters.us-east-1.amazonaws.com:8111'],
            'credentials' => $this->createMockCredentials(),
        ];

        // This should not throw an exception and should use NullLogger
        $client = new AmazonDaxClient($config);

        // No logs should be recorded since NullLogger is used
        $this->assertFalse($this->testHandler->hasRecords(Level::Debug));

        $client->close();
    }

    public function testClusterManagerLogsInitialization(): void
    {
        $config = [
            'region' => 'us-east-1',
            'endpoints' => ['dax://test-cluster.abc123.dax-clusters.us-east-1.amazonaws.com:8111'],
        ];

        $clusterManager = new ClusterManager($config, $this->logger);

        // Verify initialization logs
        $this->assertTrue($this->testHandler->hasInfoRecords());
        $this->assertTrue($this->testHandler->hasRecord('Initializing DAX cluster manager', Level::Info));

        $clusterManager->close();

        // Verify close logs
        $this->assertTrue($this->testHandler->hasRecord('Closing DAX cluster manager', Level::Info));
        $this->assertTrue($this->testHandler->hasRecord('DAX cluster manager closed successfully', Level::Debug));
    }

    public function testClusterManagerLogsErrors(): void
    {
        // Invalid configuration to trigger error
        $config = [
            'region' => 'us-east-1',
            'endpoints' => [], // Empty endpoints should cause error
        ];

        $this->expectException(\Dax\Exception\DaxException::class);

        try {
            new ClusterManager($config, $this->logger);
        } catch (\Exception $e) {
            // Verify error logging occurred
            $this->assertTrue($this->testHandler->hasErrorRecords());
            $this->assertTrue($this->testHandler->hasRecord('Failed to initialize DAX cluster manager', Level::Error));
            throw $e;
        }
    }

    public function testLoggerContextInformation(): void
    {
        $config = [
            'region' => 'us-west-2',
            'endpoints' => [
                'dax://cluster1.abc123.dax-clusters.us-west-2.amazonaws.com:8111',
                'dax://cluster2.abc123.dax-clusters.us-west-2.amazonaws.com:8111',
            ],
        ];

        $clusterManager = new ClusterManager($config, $this->logger);

        // Check that context information is logged
        $records = $this->testHandler->getRecords();
        $initRecord = null;

        foreach ($records as $record) {
            if ($record['message'] === 'Initializing DAX cluster manager') {
                $initRecord = $record;
                break;
            }
        }

        $this->assertNotNull($initRecord);
        $this->assertArrayHasKey('context', $initRecord);
        $this->assertArrayHasKey('region', $initRecord['context']);
        $this->assertEquals('us-west-2', $initRecord['context']['region']);

        $clusterManager->close();
    }
}
