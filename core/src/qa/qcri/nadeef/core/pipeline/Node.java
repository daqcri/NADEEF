/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    @SuppressWarnings("unchecked")
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
        return operator.getPercentage();
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

    /**
     * Gets the current progress percentage.
     * @return progress percentage.
     */
    void reset() {
        operator.reset();
    }

    //</editor-fold>
}
