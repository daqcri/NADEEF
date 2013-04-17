/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

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
    private LinkedList<Node> nodeList;
    private LinkedList<String> keyList;
    private int currentFlowPosition;
    private FlowState state;
    private String outputKey;
    private String inputKey;

    /**
     * Constructor.
     */
    public Flow() {
        nodeList = new LinkedList();
        keyList = new LinkedList();
        currentFlowPosition = 0;
        state = FlowState.Ready;
    }

    //<editor-fold desc="Public methods">
    public String getInputKey() {
        return inputKey;
    }

    public void setInputKey(String inputKey) {
        this.inputKey = inputKey;
    }

    public synchronized void addNode(Node node, int index) {
        nodeList.add(index, node);
    }

    public synchronized void addNode(Node node) {
        nodeList.add(node);
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
        outputKey = keyList.peekLast();
    }

    public FlowState getState() {
        return state;
    }

    public String getLastOutputKey() {
        return outputKey;
    }
    //</editor-fold>
}
