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

package qa.qcri.nadeef.lab.holisticCleaning;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Operation;
import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import qa.qcri.nadeef.core.pipeline.FixDecisionMaker;
import qa.qcri.nadeef.tools.Tracer;

import java.util.*;

/**
 * Holistic Cleaning algorithm based on paper
 * Holistic Data Cleaning: Putting Violations Into Context
 * (http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=6544847&tag=1)
 */
public class HolisticCleaning extends FixDecisionMaker {
    private Tracer tracer = Tracer.getTracer(this.getClass());
    private HashMap<Cell, HashSet<Fix>> graphMap = Maps.newHashMap();

    /**
     * CountPair is used to generate a Min/Max heap with {@link PriorityQueue}.
     */
    class CountPair<T> implements Comparable<CountPair> {
        T cell;
        int count;

        @Override
        public int compareTo(CountPair o) {
            // turns minHeap to maxHeap
            return -Integer.compare(count, o.count);
        }

        public CountPair(T cell, int count) {
            this.cell = cell;
            this.count = count;
        }
    }

    public HolisticCleaning(ExecutionContext context) {
        super(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Fix> decide(Collection<Fix> fixes) {
        // generate the graph
        for (Fix fix : fixes) {
            Cell cell = fix.getLeft();
            if (graphMap.containsKey(cell)) {
                HashSet<Fix> relatedFix = graphMap.get(cell);
                relatedFix.add(fix);
            } else {
                HashSet<Fix> relatedFix = Sets.newHashSet();
                relatedFix.add(fix);
                graphMap.put(cell, relatedFix);
            }

            if (!fix.isConstantAssign()) {
                Cell rightCell = fix.getRight();
                if (graphMap.containsKey(rightCell)) {
                    HashSet<Fix> relatedFix = graphMap.get(rightCell);
                    relatedFix.add(fix);
                } else {
                    HashSet<Fix> relatedFix = Sets.newHashSet();
                    relatedFix.add(fix);
                    graphMap.put(cell, relatedFix);
                }
            }
        }

        // initialize the heap by count of edges
        // Here instead of using MVC, we use a greedy approach
        // to get the vertexes in the order of hyper-edge counts.
        PriorityQueue<CountPair<Cell>> maxHeap = new PriorityQueue<>();
        HashMap<Cell, CountPair<Cell>> heapIndex = Maps.newHashMap();

        for (Cell cell: graphMap.keySet()) {
            CountPair<Cell> pair = new CountPair<>(cell, graphMap.get(cell).size());
            maxHeap.add(pair);
            heapIndex.put(cell, pair);
        }

        ArrayList<Fix> result = Lists.newArrayList();

        // Inner loop, in this implementation we don't have an outer-loop
        while (!maxHeap.isEmpty()) {
            HashSet<Fix> repairContext = Sets.newHashSet();
            CountPair<Cell> topCell = maxHeap.poll();
            // generate the frontier using BFS
            HashSet<Cell> frontier = generateFrontier(topCell.cell);

            // remove frontier vertexes from the graph
            for (Cell cell : frontier) {
                repairContext.addAll(graphMap.get(cell));
                CountPair<Cell> pair = heapIndex.get(cell);
                // remove from the heap
                maxHeap.remove(pair);
            }

            result.addAll(determine(repairContext));
        }
        return result;
    }


    private HashSet<Cell> generateFrontier(Cell topCell) {
        HashSet<Cell> result = Sets.newHashSet();
        result.add(topCell);

        Queue<Fix> queue = new LinkedList<>(graphMap.get(topCell));
        while (!queue.isEmpty()) {
            Fix fix = queue.poll();
            Cell cell = fix.getLeft();
            if (!result.contains(cell)) {
                result.add(cell);
                queue.addAll(graphMap.get(cell));
            }

            if (!fix.isConstantAssign() && !result.contains(cell)) {
                result.add(fix.getRight());
                queue.addAll(graphMap.get(cell));
            }
        }
        return result;

    }


    private List<Fix> qpDetermine(HashSet<Fix> repairContext) {
        return new GurobiSolver().solve(repairContext);
    }

    private List<Fix> determine(HashSet<Fix> repairContext) {
        boolean hasOnlyEq = true;
        for (Fix fix : repairContext) {
            if (fix.getOperation() != Operation.EQ &&
                fix.getOperation() != Operation.NEQ
            ) {
                hasOnlyEq = false;
                break;
            }
        }

        if (hasOnlyEq) {
            // executing vfm
            return new VFMSolver().solve(repairContext);
        }

        // executing QP
        return new GurobiSolver().solve(repairContext);
    }
}