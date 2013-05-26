/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.operator.Operator;
import qa.qcri.nadeef.tools.Tracer;

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
 * Currently the design only allows for one line of connected nodes.
 */
public class Flow {

    //<editor-fold desc="Private fields">
    private List<Node> nodeList;
    private List<String> keyList;
    private int currentFlowPosition;
    private FlowState state;
    private String name;
    private String inputKey;
    private Thread thread;
    private static Tracer tracer = Tracer.getTracer(Flow.class);
    private boolean forceClose = false;

    //</editor-fold>

    //<editor-fold desc="Constructor">
    /**
     * Constructor.
     * @param name the name of the flow.
     */
    public Flow(String name) {
        this.name = name;
        nodeList = Lists.newArrayList();
        keyList = Lists.newArrayList();
        currentFlowPosition = 0;
        state = FlowState.Ready;
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">

    /**
     * Resets the flow.
     */
    public void reset() {
        if (thread != null && thread.isAlive()) {
            throw new RuntimeException("Flow cannot be reset during running.");
        }
        state = FlowState.Ready;
        currentFlowPosition = 0;
    }

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
    public Flow addNode(Node node, int index) {
        if (thread != null && thread.isAlive()) {
            throw new RuntimeException("Flow cannot be modified during running.");
        }

        nodeList.add(index, node);
        return this;
    }

    /**
     * Adds an operator in the flow.
     * @param operator operator.
     * @return Flow itself.
     */
    public Flow addNode(Operator operator) {
        if (thread != null && thread.isAlive()) {
            throw new RuntimeException("Flow cannot be modified during running.");
        }

        nodeList.add(new Node(operator, operator.getClass().getSimpleName()));
        return this;
    }

    /**
     * Starts the flow.
     */
    public void start() {
        if (thread != null && thread.isAlive()) {
            tracer.info("Flow " + name + " is already started.");
        }

        reset();
        state = FlowState.Running;
        thread = new Thread() {
            public void run() {
                try {
                    String inputKey_ = inputKey;
                    for (int i = currentFlowPosition; i < nodeList.size(); i ++) {
                        if (i != 0) {
                            inputKey_ = keyList.get(i - 1);
                        }

                        if (forceClose) {
                            break;
                        }

                        Node node = nodeList.get(i);
                        if (node.canExecute(inputKey_)) {
                            String outputKey = node.execute(inputKey_);
                            keyList.add(currentFlowPosition, outputKey);
                            currentFlowPosition ++;
                        } else {
                            state = FlowState.Stopped;
                            throw
                                new IllegalStateException(
                                    "Flow stops at node: " + currentFlowPosition
                                );
                        }
                    }
                } catch (Exception ex) {
                    tracer.err("Flow stops at node" + currentFlowPosition, ex);
                } finally {
                    state = FlowState.Stopped;
                }
            }
        };
        thread.start();
    }

    /**
     * Wait until the flow is finished. It is a blocking operation.
     * @throws InterruptedException
     */
    public void waitUntilFinish() {
        try {
            if (thread != null) {
                thread.join();
            }
        } catch (InterruptedException ex) {
            tracer.err("Flow " + name + " is interrupted.", ex);
        }
    }

    /**
     * Forces the Flow to be closed.
     */
    public synchronized void forceClose() {
        forceClose = true;
    }

    /**
     * Gets the last output key.
     * @return output key.
     */
    public String getCurrentOutputKey() {
        if (currentFlowPosition == 0) {
            return null;
        }
        return keyList.get(currentFlowPosition - 1);
    }

    /**
     * Returns <code>True</code> when the flow is still running.
     * @return <code>True</code> when the flow is still running.
     */
    public boolean isRunning() {
        return state == FlowState.Running;
    }
    //</editor-fold>
}
