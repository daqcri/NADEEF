/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.tools.Tracer;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
    private static Tracer tracer = Tracer.getTracer(Flow.class);

    private List<Node> nodeList;
    private List<Integer> weights;
    private List<String> keyList;
    private int currentFlowPosition;
    private FlowState state;
    private String name;
    private String inputKey;
    private Thread thread;
    private boolean forceStop = false;
    private long elapsedTime;
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
        weights = Lists.newArrayList();
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
        for (int i = 0; i < nodeList.size(); i ++) {
            nodeList.get(i).reset();
        }
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
        return addNode(node, index, 1);
    }

    /**
     * Adds an operator in the flow.
     * @param operator operator.
     * @param weight percentage weight of the flow.
     * @return Flow itself.
     */
    public Flow addNode(Operator operator, int weight) {
        return addNode(
            new Node(operator, operator.getClass().getSimpleName()),
            nodeList.size(),
            weight
        );
    }

    /**
     * Adds an operator in the flow.
     * @param operator operator.
     * @return Flow itself.
     */
    public Flow addNode(Operator operator) {
        return addNode(
            new Node(operator, operator.getClass().getSimpleName()),
            nodeList.size(),
            1
        );
    }

    /**
     * Adds an node in the flow.
     * @param node flow node.
     * @param index index.
     * @param weight percentage weight.
     * @return Flow itself.
     */
    public Flow addNode(Node node, int index, int weight) {
        if (thread != null && thread.isAlive()) {
            throw new RuntimeException("Flow cannot be modified during running.");
        }

        Preconditions.checkArgument(
            weight <= 100 && weight > 0,
            "Weight needs to be between [1, 100]."
        );
        nodeList.add(index, node);
        weights.add(weight);
        return this;
    }

    /**
     * Starts the flow.
     */
    public void start() {
        if (thread != null && thread.isAlive()) {
            tracer.info("Flow " + name + " is already started.");
        }

        state = FlowState.Running;
        thread = new Thread() {
            public void run() {
                Stopwatch stopwatch = null;
                try {
                    stopwatch = new Stopwatch().start();
                    String inputKey_ = inputKey;
                    for (int i = currentFlowPosition; i < nodeList.size(); i ++) {
                        if (i != 0) {
                            inputKey_ = keyList.get(i - 1);
                        }

                        if (forceStop) {
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
                    elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                    for (int i = 0; i < nodeList.size(); i ++) {
                        nodeList.get(i).interrupt();
                    }
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
    public synchronized void forceStop() {
        forceStop = true;
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

    /**
     * Gets elapsed time for this flow.
     * @return elapsed time.
     */
    public long getElapsedTime() {
        return elapsedTime;
    }

    /**
     * Gets the current progress percentage.
     */
    public double getPercentage() {
        int percentage = 0;
        double weightSum = 0;
        int weight;
        for (int i = 0; i < weights.size(); i ++) {
            weight = weights.get(i);
            weightSum += weight;
            Node node = nodeList.get(i);
            percentage += node.getPercentage() * weight;
        }
        return (double)percentage / weightSum;
    }
    //</editor-fold>
}
