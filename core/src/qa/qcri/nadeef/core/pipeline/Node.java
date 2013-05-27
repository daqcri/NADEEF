/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import java.util.UUID;

/**
 * A component runs in the pipeline.
 */
public class Node {

    //<editor-fold desc="Private fields">
    private UUID uid;
    private String name;
    private Operator operator;

    private synchronized String generateKey() {
        return System.currentTimeMillis() + "_name_" + uid.toString();
    }

    //</editor-fold>

    //<editor-fold desc="Public methods">

    /**
     * Constructor.
     * @param operator Operator
     */
    public Node(Operator operator, String name) {
        // currently a node contains only one operator.
        this.operator = operator;
        uid = UUID.randomUUID();
        this.name = name;
    }

    public boolean canExecute(String key) {
        NodeCacheManager nodeCache = NodeCacheManager.getInstance();
        return operator.canExecute(nodeCache.tease(key));
    }

    public String execute(String key) {
        Object result = null;
        if (operator == null) {
            throw new NullPointerException("Operator is null.");
        }

        NodeCacheManager nodeCache = NodeCacheManager.getInstance();
        Object input = nodeCache.get(key);
        // TODO: adds exception handling on node.
        if (operator.canExecute(input)) {
            try {
                operator.setPercentage(0.0f);
                result = operator.execute(input);
                operator.setPercentage(1.0f);
                String newKey = generateKey();
                nodeCache.put(newKey, result, 1);
                return newKey;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return null;
    }

    /**
     * Gets the current progress percentage.
     * @return progress percentage.
     */
    public double getPercentage() {
        return operator.percentage();
    }

    /**
     * Gets the node name.
     * @return Node name
     */
    public String getName() {
        return name;
    }

    /**
     * Interrupt is called in situation when the operator needs to shutdown during running.
     */
    void interrupt() {
        operator.interrupt();
    }
    //</editor-fold>
}
