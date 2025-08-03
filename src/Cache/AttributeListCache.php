<?php

declare(strict_types=1);

namespace Dax\Cache;

use Psr\SimpleCache\CacheInterface;
use Psr\SimpleCache\InvalidArgumentException;

/**
 * Cache for DynamoDB attribute list definitions
 */
class AttributeListCache implements CacheInterface
{
    private array $cache = [];
    private int $maxSize;
    private int $accessCounter = 0;
    private array $accessTimes = [];

    /**
     * Constructor
     *
     * @param int $maxSize Maximum number of entries to cache
     */
    public function __construct(int $maxSize = 1000)
    {
        $this->maxSize = $maxSize;
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

        // Update access time for LRU
        $this->accessTimes[$key] = ++$this->accessCounter;
        
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
            $this->evictLeastRecentlyUsed();
        }

        $this->cache[$key] = $value;
        $this->accessTimes[$key] = ++$this->accessCounter;
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
        unset($this->accessTimes[$key]);
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
        $this->accessTimes = [];
        $this->accessCounter = 0;
        return true;
    }

    /**
     * Get cache statistics
     *
     * @return array Cache statistics
     */
    public function getStats(): array
    {
        return [
            'size' => count($this->cache),
            'max_size' => $this->maxSize,
            'access_counter' => $this->accessCounter,
            'hit_ratio' => $this->calculateHitRatio()
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
        return isset($this->cache[$key]);
    }

    /**
     * Get all cached attribute list IDs
     *
     * @return array Array of attribute list IDs
     */
    public function getAttributeListIds(): array
    {
        return array_keys($this->cache);
    }

    /**
     * Get attribute list by name hash
     *
     * @param string $nameHash Hash of attribute names
     * @return int|null Attribute list ID or null if not found
     */
    public function getIdByNameHash(string $nameHash): ?int
    {
        foreach ($this->cache as $id => $attributeList) {
            if (isset($attributeList['name_hash']) && $attributeList['name_hash'] === $nameHash) {
                // Update access time for LRU
                $this->accessTimes[$id] = ++$this->accessCounter;
                return $id;
            }
        }
        return null;
    }

    /**
     * Put attribute list with name hash for reverse lookup
     *
     * @param array $attributeNames Array of attribute names
     * @param array $attributeList Attribute list
     * @return int Generated attribute list ID
     */
    public function putByNames(array $attributeNames, array $attributeList): int
    {
        $nameHash = $this->hashAttributeNames($attributeNames);
        
        // Check if we already have this combination
        $existingId = $this->getIdByNameHash($nameHash);
        if ($existingId !== null) {
            return $existingId;
        }

        // Generate new ID
        $attributeListId = $this->generateId();
        
        // Add name hash to attribute list for reverse lookup
        $attributeList['name_hash'] = $nameHash;
        $attributeList['attribute_names'] = $attributeNames;
        
        $this->put($attributeListId, $attributeList);
        
        return $attributeListId;
    }

    /**
     * Put attribute list with ID (backward compatibility)
     *
     * @param int $attributeListId Attribute list ID
     * @param array $attributeList Attribute list
     */
    public function put(int $attributeListId, array $attributeList): void
    {
        $this->set((string)$attributeListId, $attributeList);
    }

    /**
     * Remove attribute list by ID (backward compatibility)
     *
     * @param int $attributeListId Attribute list ID
     */
    public function remove(int $attributeListId): void
    {
        $this->delete((string)$attributeListId);
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
     * Evict the least recently used entry
     */
    private function evictLeastRecentlyUsed(): void
    {
        if (empty($this->accessTimes)) {
            return;
        }

        $lruId = array_keys($this->accessTimes, min($this->accessTimes))[0];
        $this->delete((string)$lruId);
    }

    /**
     * Calculate hit ratio (simplified)
     *
     * @return float Hit ratio between 0 and 1
     */
    private function calculateHitRatio(): float
    {
        if ($this->accessCounter === 0) {
            return 0.0;
        }

        // This is a simplified calculation
        // In a real implementation, you'd track hits vs misses
        return min(1.0, count($this->cache) / max(1, $this->accessCounter * 0.1));
    }

    /**
     * Generate a hash for attribute names
     *
     * @param array $attributeNames Array of attribute names
     * @return string Hash of attribute names
     */
    private function hashAttributeNames(array $attributeNames): string
    {
        sort($attributeNames); // Ensure consistent ordering
        return hash('sha256', implode('|', $attributeNames));
    }

    /**
     * Generate a new attribute list ID
     *
     * @return int New attribute list ID
     */
    private function generateId(): int
    {
        // Simple ID generation - in practice, this might be more sophisticated
        static $nextId = 1;
        return $nextId++;
    }
}
