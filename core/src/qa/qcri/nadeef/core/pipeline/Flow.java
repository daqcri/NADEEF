/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.operator.Operator;

import java.util.LinkedList;
import java.util.List;

/**
 * Flow state.
 */
enum FlowState {
    Ready,
    Running,
    Stopped
}

/**
 * Flow contains a series of Nodes which can be connected and executed in sequence.
 * TODO: currently the design only allows for one line of connected nodes.
 */
public class Flow {
    private List<Node> nodeList;
    private List<String> keyList;
    private int currentFlowPosition;
    private FlowState state;
    private String outputKey;
    private String inputKey;

    /**
     * Constructor.
     */
    public Flow() {
        nodeList = Lists.newArrayList();
        keyList = Lists.newArrayList();
        currentFlowPosition = 0;
        state = FlowState.Ready;
    }

    //<editor-fold desc="Public methods">

    /**
     * Gets the input key of the flow.
     * @return input key.
     */
    public String getInputKey() {
        return inputKey;
    }

    /**
     * Sets the input key of the flow.
     * @param inputKey input key.
     */
    public Flow setInputKey(String inputKey) {
        this.inputKey = inputKey;
        return this;
    }

    /**
     * Adds a new node in the flow.
     * @param node node.
     * @param index index.
     */
    public synchronized Flow addNode(Node node, int index) {
        nodeList.add(index, node);
        return this;
    }

    public synchronized Flow addNode(Node node) {
        nodeList.add(node);
        return this;
    }

    public synchronized Flow addNode(Operator operator, String name) {
        nodeList.add(new Node(operator, name));
        return this;
    }

    public synchronized void removeNode(Node node, int index) {
        nodeList.remove(index);
    }

    public synchronized void start() {
        if (currentFlowPosition == 0) {
            state = FlowState.Running;
        }

        String inputKey = this.inputKey;
        for (int i = currentFlowPosition; i < nodeList.size(); i ++) {
            if (i != 0) {
                inputKey = keyList.get(i - 1);
            }
            Node node = nodeList.get(i);
            if (node.canExecute(inputKey)) {
                String outputKey = node.execute(inputKey);
                keyList.add(currentFlowPosition, outputKey);
                currentFlowPosition ++;
            } else {
                state = FlowState.Stopped;
                throw new IllegalStateException("Flow stops at node: " + currentFlowPosition);
            }
        }

        state = FlowState.Ready;
        outputKey = keyList.get(currentFlowPosition - 1);
        currentFlowPosition = 0;
    }

    public FlowState getState() {
        return state;
    }

    public String getLastOutputKey() {
        return outputKey;
    }
    //</editor-fold>
}
