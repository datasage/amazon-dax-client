<?php

declare(strict_types=1);

namespace Amazon\Dax\Cache;

use Psr\SimpleCache\CacheInterface;
use Psr\SimpleCache\InvalidArgumentException;

/**
 * Cache for DynamoDB table key schemas with TTL support
 */
class KeySchemaCache implements CacheInterface
{
    private array $cache = [];
    private array $timestamps = [];
    private int $maxSize;
    private int $ttlMillis;

    /**
     * Constructor
     *
     * @param int $maxSize Maximum number of entries to cache
     * @param int $ttlMillis Time-to-live in milliseconds
     */
    public function __construct(int $maxSize = 1000, int $ttlMillis = 60000)
    {
        $this->maxSize = $maxSize;
        $this->ttlMillis = $ttlMillis;
    }

    /**
     * Fetches a value from the cache.
     *
     * @param string $key The unique key of this item in the cache.
     * @param mixed $default Default value to return if the key does not exist.
     * @return mixed The value of the item from the cache, or $default in case of cache miss.
     * @throws InvalidArgumentException MUST be thrown if the $key string is not a legal value.
     */
    public function get(string $key, mixed $default = null): mixed
    {
        $this->validateKey($key);
        
        if (!isset($this->cache[$key])) {
            return $default;
        }

        // Check if entry has expired
        $timestamp = $this->timestamps[$key];
        $currentTime = $this->getCurrentTimeMillis();
        
        if ($currentTime - $timestamp > $this->ttlMillis) {
            $this->delete($key);
            return $default;
        }

        return $this->cache[$key];
    }

    /**
     * Persists data in the cache, uniquely referenced by a key with an optional expiration TTL time.
     *
     * @param string $key The key of the item to store.
     * @param mixed $value The value of the item to store, must be serializable.
     * @param null|int|\DateInterval $ttl Optional. The TTL value of this item.
     * @return bool True on success and false on failure.
     * @throws InvalidArgumentException MUST be thrown if the $key string is not a legal value.
     */
    public function set(string $key, mixed $value, null|int|\DateInterval $ttl = null): bool
    {
        $this->validateKey($key);
        
        // Ensure we don't exceed max size
        if (count($this->cache) >= $this->maxSize && !isset($this->cache[$key])) {
            $this->evictOldest();
        }

        $this->cache[$key] = $value;
        $this->timestamps[$key] = $this->getCurrentTimeMillis();
        return true;
    }

    /**
     * Delete an item from the cache by its unique key.
     *
     * @param string $key The unique cache key of the item to delete.
     * @return bool True if the item was successfully removed. False if there was an error.
     * @throws InvalidArgumentException MUST be thrown if the $key string is not a legal value.
     */
    public function delete(string $key): bool
    {
        $this->validateKey($key);
        
        $existed = isset($this->cache[$key]);
        unset($this->cache[$key]);
        unset($this->timestamps[$key]);
        return $existed;
    }

    /**
     * Wipes clean the entire cache's keys.
     *
     * @return bool True on success and false on failure.
     */
    public function clear(): bool
    {
        $this->cache = [];
        $this->timestamps = [];
        return true;
    }

    /**
     * Get cache statistics
     *
     * @return array Cache statistics
     */
    public function getStats(): array
    {
        $currentTime = $this->getCurrentTimeMillis();
        $expiredCount = 0;

        foreach ($this->timestamps as $timestamp) {
            if ($currentTime - $timestamp > $this->ttlMillis) {
                $expiredCount++;
            }
        }

        return [
            'size' => count($this->cache),
            'max_size' => $this->maxSize,
            'ttl_millis' => $this->ttlMillis,
            'expired_count' => $expiredCount
        ];
    }

    /**
     * Obtains multiple cache items by their unique keys.
     *
     * @param iterable<string> $keys A list of keys that can be obtained in a single operation.
     * @param mixed $default Default value to return for keys that do not exist.
     * @return iterable<string, mixed> A list of key => value pairs.
     * @throws InvalidArgumentException MUST be thrown if $keys is neither an array nor a Traversable.
     */
    public function getMultiple(iterable $keys, mixed $default = null): iterable
    {
        $result = [];
        foreach ($keys as $key) {
            $result[$key] = $this->get($key, $default);
        }
        return $result;
    }

    /**
     * Persists a set of key => value pairs in the cache, with an optional TTL.
     *
     * @param iterable $values A list of key => value pairs for a multiple-set operation.
     * @param null|int|\DateInterval $ttl Optional. The TTL value of this item.
     * @return bool True on success and false on failure.
     * @throws InvalidArgumentException MUST be thrown if $values is neither an array nor a Traversable.
     */
    public function setMultiple(iterable $values, null|int|\DateInterval $ttl = null): bool
    {
        $success = true;
        foreach ($values as $key => $value) {
            if (!$this->set($key, $value, $ttl)) {
                $success = false;
            }
        }
        return $success;
    }

    /**
     * Deletes multiple cache items in a single operation.
     *
     * @param iterable<string> $keys A list of string-based keys to be deleted.
     * @return bool True if the items were successfully removed. False if there was an error.
     * @throws InvalidArgumentException MUST be thrown if $keys is neither an array nor a Traversable.
     */
    public function deleteMultiple(iterable $keys): bool
    {
        $success = true;
        foreach ($keys as $key) {
            if (!$this->delete($key)) {
                $success = false;
            }
        }
        return $success;
    }

    /**
     * Determines whether an item is present in the cache.
     *
     * @param string $key The cache item key.
     * @return bool
     * @throws InvalidArgumentException MUST be thrown if the $key string is not a legal value.
     */
    public function has(string $key): bool
    {
        $this->validateKey($key);
        return $this->get($key) !== null;
    }

    /**
     * Get all cached table names
     *
     * @return array Array of table names
     */
    public function getTableNames(): array
    {
        return array_keys($this->cache);
    }

    /**
     * Put key schema for a table (backward compatibility)
     *
     * @param string $tableName Table name
     * @param array $keySchema Key schema
     */
    public function put(string $tableName, array $keySchema): void
    {
        $this->set($tableName, $keySchema);
    }

    /**
     * Remove key schema for a table (backward compatibility)
     *
     * @param string $tableName Table name
     */
    public function remove(string $tableName): void
    {
        $this->delete($tableName);
    }

    /**
     * Validates a cache key according to PSR-16 requirements
     *
     * @param string $key The cache key to validate
     * @throws InvalidArgumentException If the key is not valid
     */
    private function validateKey(string $key): void
    {
        if ($key === '') {
            throw new class('Cache key cannot be empty') extends \InvalidArgumentException implements InvalidArgumentException {};
        }
        
        if (preg_match('/[{}()\/@:]/', $key)) {
            throw new class('Cache key contains reserved characters') extends \InvalidArgumentException implements InvalidArgumentException {};
        }
    }

    /**
     * Evict the oldest entry from the cache
     */
    private function evictOldest(): void
    {
        if (empty($this->timestamps)) {
            return;
        }

        $oldestTable = array_keys($this->timestamps, min($this->timestamps))[0];
        $this->delete($oldestTable);
    }

    /**
     * Get current time in milliseconds
     *
     * @return int Current time in milliseconds
     */
    private function getCurrentTimeMillis(): int
    {
        return (int)(microtime(true) * 1000);
    }
}
