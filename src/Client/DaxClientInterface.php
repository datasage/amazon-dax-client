<?php

declare(strict_types=1);

namespace Dax\Client;

/**
 * Interface for DAX client operations
 */
interface DaxClientInterface
{
    /**
     * Batch get items from multiple tables
     *
     * @param array $requestItems Array of table names and their respective keys
     * @return array Response containing items and unprocessed keys
     */
    public function batchGetItem(array $requestItems): array;

    /**
     * Batch write items to multiple tables
     *
     * @param array $requestItems Array of table names and their respective write requests
     * @return array Response containing unprocessed items
     */
    public function batchWriteItem(array $requestItems): array;

    /**
     * Delete an item from a table
     *
     * @param string $tableName Name of the table
     * @param array $key Primary key of the item to delete
     * @param array $options Additional options for the delete operation
     * @return array Response from the delete operation
     */
    public function deleteItem(string $tableName, array $key, array $options = []): array;

    /**
     * Get an item from a table
     *
     * @param string $tableName Name of the table
     * @param array $key Primary key of the item to retrieve
     * @param array $options Additional options for the get operation
     * @return array Response containing the item
     */
    public function getItem(string $tableName, array $key, array $options = []): array;

    /**
     * Put an item into a table
     *
     * @param string $tableName Name of the table
     * @param array $item Item to put into the table
     * @param array $options Additional options for the put operation
     * @return array Response from the put operation
     */
    public function putItem(string $tableName, array $item, array $options = []): array;

    /**
     * Query items from a table
     *
     * @param string $tableName Name of the table
     * @param array $options Query options including key conditions
     * @return array Response containing items and pagination info
     */
    public function query(string $tableName, array $options = []): array;

    /**
     * Scan items from a table
     *
     * @param string $tableName Name of the table
     * @param array $options Scan options including filters
     * @return array Response containing items and pagination info
     */
    public function scan(string $tableName, array $options = []): array;

    /**
     * Update an item in a table
     *
     * @param string $tableName Name of the table
     * @param array $key Primary key of the item to update
     * @param array $options Update options including expressions
     * @return array Response from the update operation
     */
    public function updateItem(string $tableName, array $key, array $options = []): array;

    /**
     * Close the client and clean up resources
     */
    public function close(): void;
}
