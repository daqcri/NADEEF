/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.pipeline.NodeCacheManager;

/**
 * Bootstrapping Nadeef.
 */
public class Bootstrap {
    private static NodeCacheManager nodeCacheManager = null;
    private static boolean isStarted;

    /**
     * Initialize the Nadeef infrastructure.
     */
    public static synchronized void Start() {
        nodeCacheManager = NodeCacheManager.getInstance();
        isStarted = true;
    }

    /**
     * Gets the <code>NodeCacheManger</code> instance.
     * @return <Code>NodeCacheManager</Code> instance.
     */
    public static NodeCacheManager getNodeCacheManager() {
        if (!isStarted) {
            throw new IllegalStateException("Nadeef is not bootstrapped.");
        }
        return nodeCacheManager;
    }
}
