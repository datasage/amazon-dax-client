<?php

declare(strict_types=1);

namespace Amazon\Dax\Tests\Unit;

use Amazon\Dax\AmazonDaxClient;
use Amazon\Dax\Exception\DaxException;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for AmazonDaxClient
 */
class AmazonDaxClientTest extends TestCase
{
    public function testConstructorWithValidConfig(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $this->assertInstanceOf(AmazonDaxClient::class, $client);
    }

    public function testConstructorWithEndpoints(): void
    {
        $config = [
            'endpoints' => ['dax://node1.cluster.dax-clusters.us-east-1.amazonaws.com'],
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $this->assertInstanceOf(AmazonDaxClient::class, $client);
    }

    public function testConstructorThrowsExceptionWithoutEndpoints(): void
    {
        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Either endpoint_url or endpoints must be provided');

        new AmazonDaxClient(['region' => 'us-east-1']);
    }

    public function testConstructorThrowsExceptionWithBothEndpointTypes(): void
    {
        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Cannot specify both endpoint_url and endpoints');

        new AmazonDaxClient([
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'endpoints' => ['dax://node1.cluster.dax-clusters.us-east-1.amazonaws.com'],
            'region' => 'us-east-1'
        ]);
    }

    public function testFactoryMethod(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = AmazonDaxClient::factory($config);
        $this->assertInstanceOf(AmazonDaxClient::class, $client);
    }

    public function testGetItemThrowsExceptionWhenClosed(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $client->close();

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Client has been closed');

        $client->getItem('TestTable', ['id' => ['S' => 'test']]);
    }

    public function testPutItemThrowsExceptionWhenClosed(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $client->close();

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Client has been closed');

        $client->putItem('TestTable', ['id' => ['S' => 'test'], 'data' => ['S' => 'value']]);
    }

    public function testDeleteItemThrowsExceptionWhenClosed(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $client->close();

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Client has been closed');

        $client->deleteItem('TestTable', ['id' => ['S' => 'test']]);
    }

    public function testUpdateItemThrowsExceptionWhenClosed(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $client->close();

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Client has been closed');

        $client->updateItem('TestTable', ['id' => ['S' => 'test']], ['UpdateExpression' => 'SET #data = :val']);
    }

    public function testBatchGetItemThrowsExceptionWhenClosed(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $client->close();

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Client has been closed');

        $client->batchGetItem(['TestTable' => ['Keys' => [['id' => ['S' => 'test']]]]]);
    }

    public function testBatchWriteItemThrowsExceptionWhenClosed(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $client->close();

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Client has been closed');

        $client->batchWriteItem(['TestTable' => [['PutRequest' => ['Item' => ['id' => ['S' => 'test']]]]]]);
    }

    public function testQueryThrowsExceptionWhenClosed(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $client->close();

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Client has been closed');

        $client->query('TestTable', ['KeyConditionExpression' => 'id = :id']);
    }

    public function testScanThrowsExceptionWhenClosed(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $client->close();

        $this->expectException(DaxException::class);
        $this->expectExceptionMessage('Client has been closed');

        $client->scan('TestTable');
    }

    public function testCloseCanBeCalledMultipleTimes(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com',
            'region' => 'us-east-1'
        ];

        $client = new AmazonDaxClient($config);
        $client->close();
        $client->close(); // Should not throw exception

        $this->assertTrue(true); // Test passes if no exception is thrown
    }

    public function testDefaultConfiguration(): void
    {
        $config = [
            'endpoint_url' => 'dax://test.cluster.dax-clusters.us-east-1.amazonaws.com'
        ];

        $client = new AmazonDaxClient($config);
        $this->assertInstanceOf(AmazonDaxClient::class, $client);
    }
}
