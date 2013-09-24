/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.UUID;

/**
 * Node Cache manager manages the input/output of the node execution.
 * It is basically a pair-value container.
 *
 * @author Si Yin <siyin@qf.org.qa>
 */
public class NodeCacheManager {
    private static HashMap<String, Object> cachePool;
    private static HashMap<String, Integer> refPool;

    //<editor-fold desc="Singleton">
    private static final NodeCacheManager instance = new NodeCacheManager();

    private NodeCacheManager() {
        cachePool = Maps.newHashMap();
        refPool = Maps.newHashMap();
    }

    /**
     * Singleton instance.
     */
    public static NodeCacheManager getInstance() {
        return instance;
    }
    //</editor-fold>

    /**
     * Add key-value pair in the container.
     * @param key value key.
     * @param value value.
     */
    public synchronized void put(String key, Object value) {
        if (cachePool.containsKey(key)) {
            throw new IllegalStateException("Invalid key, key already existed in the cache.");
        }

        cachePool.put(key, value);
        refPool.put(key, 1);
    }

    /**
     * Add a value with generated unique key.
     * @param value new object.
     * @return generated key.
     */
    public synchronized String put(Object value) {
        UUID uuId = UUID.randomUUID();
        String key = uuId.toString();
        put(key, value);
        return key;
    }

    /**
     * Add a value with generated unique key.
     * @param value new object.
     * @param lifeCount life count.
     * @return generated key.
     */
    public synchronized String put(Object value, int lifeCount) {
        UUID uuId = UUID.randomUUID();
        String key = uuId.toString();
        put(key, value, lifeCount);
        return key;
    }

    /**
     * Add key-value pair in the container.
     * @param key value key.
     * @param value value.
     * @param lifeCount life time of the value.
     */
    public synchronized void put(String key, Object value, int lifeCount) {
        if (cachePool.containsKey(key)) {
            throw new IllegalStateException("Invalid key, key already existed in the cache.");
        }

        cachePool.put(key, value);
        refPool.put(key, lifeCount);
    }

    /**
     * Get the value from key, without reducing the life count.
     * @param key
     * @return value.
     */
    public synchronized Object tease(String key) {
        if (!cachePool.containsKey(key)) {
            throw new IllegalStateException("Invalid key, key doesn't exist in the cache.");
        }

        return cachePool.get(key);
    }

    /**
     * Get the value from the key. Clean the object from the cache once the life
     * cycle is done.
     * @param key value key.
     * @return value.
     */
    public synchronized Object get(String key) {
        Object result = tease(key);

        // clean the cache once the life is finished, to prevent memory leaking.
        int refCount = refPool.get(key) - 1;
        if (refCount == 0) {
            cachePool.remove(key);
            refPool.remove(key);
        } else {
            refPool.put(key, refCount);
        }

        return result;
    }

    /**
     * Gets the size of the cache pool.
     * @return size of the cache pool.
     */
    public int getSize() {
        return cachePool.size();
    }

    /**
     * Clear all the resources in the cache.
     */
    public void clear() {
        cachePool.clear();
        refPool.clear();
    }
}
