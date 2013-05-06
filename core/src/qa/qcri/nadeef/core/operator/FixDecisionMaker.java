/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.collect.*;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Violation;

import java.util.*;

/**
 * From the generated candidate fixes, find the good ones.
 */
public class FixDecisionMaker extends Operator<Collection<Fix>, Collection<Fix>> {

    /**
     * Execute the operator.
     *
     * @param fixes input object.
     * @return output object.
     */
    @Override
    public Collection<Fix> execute(Collection<Fix> fixes) throws Exception {
        List<HashSet<Cell>> clusters = Lists.newArrayList();
        // a map between a cell and n
        HashMap<Cell, HashSet<Cell>> clusterMap = Maps.newHashMap();
        HashMap<Cell, String> assignMap = Maps.newHashMap();
        // a map between cell and fix, used for getting the original vid.
        HashMap<Cell, Fix> fixMap = Maps.newHashMap();

        // Clustering all the fixes.
        for (Fix fix : fixes) {
            Cell leftCell = fix.getLeft();
            fixMap.put(leftCell, fix);

            if (fix.isConstantAssign()) {
                // TODO: do a statistic on the assign count.
                assignMap.put(leftCell, fix.getRightValue());
                continue;
            }

            Cell rightCell = fix.getRight();
            fixMap.put(rightCell, fix);
            if (assignMap.containsKey(leftCell)) {
                assignMap.remove(leftCell);
            }

            if (assignMap.containsKey(rightCell)) {
                assignMap.remove(rightCell);
            }

            HashSet<Cell> leftCluster = null;
            HashSet<Cell> rightCluster = null;

            // when the left column is already in a cluster
            if (clusterMap.containsKey(leftCell)) {
                leftCluster = clusterMap.get(leftCell);
                if (!leftCluster.contains(rightCell)) {
                    // union of two cluster of cell sets.
                    if (clusterMap.containsKey(rightCell)) {
                        rightCluster = clusterMap.get(rightCell);
                        for (Cell cell : rightCluster) {
                            leftCluster.add(cell);
                            clusterMap.put(cell, leftCluster);
                        }

                        for (Cell cell : rightCluster) {
                            rightCluster.remove(cell);
                        }

                        clusters.remove(rightCluster);
                    } else {
                        clusterMap.put(rightCell, leftCluster);
                        leftCluster.add(rightCell);
                    }
                }
            } else if (clusterMap.containsKey(rightCell)) {
                // when the right column is already in the cluster
                rightCluster = clusterMap.get(rightCell);
                if (!rightCluster.contains(leftCell)) {
                    // union of two cluster of cell sets.
                    if (clusterMap.containsKey(leftCell)) {
                        leftCluster = clusterMap.get(leftCell);
                        for (Cell cell : leftCluster) {
                            rightCluster.add(cell);
                            clusterMap.put(cell, rightCluster);
                        }

                        for (Cell cell : leftCluster) {
                            leftCluster.remove(cell);
                        }

                        clusters.remove(leftCluster);
                    } else {
                        clusterMap.put(leftCell, rightCluster);
                        rightCluster.add(leftCell);
                    }
                }
            } else {
                // both left and right are not in any of the cluster
                // create a new cluster of containing both.
                HashSet<Cell> cluster = Sets.newHashSet();
                cluster.add(leftCell);
                cluster.add(rightCell);
                clusterMap.put(leftCell, cluster);
                clusterMap.put(rightCell, cluster);
                clusters.add(cluster);
            }
        }

        // start to count each cluster and decide the final fix based on
        // percentage.
        List<Fix> result = Lists.newArrayList();
        // for final execution of all the fixes, we use 0 as default as the fix id.
        Fix.Builder fixBuilder = new Fix.Builder();
        for (HashSet<Cell> cluster : clusters) {
            Multiset<Object> countSet = HashMultiset.create();
            for (Cell cell : cluster) {
                countSet.add(cell.getAttributeValue());
            }

            countSet = Multisets.copyHighestCountFirst(countSet);
            Object value = countSet.iterator().next();
            for (Cell cell : cluster) {
                if (cell.getAttributeValue().equals(value)) {
                    // skip the correct value.
                    continue;
                }
                Fix originalFix = fixMap.get(cell);
                Fix newFix =
                    fixBuilder.vid(originalFix.getVid())
                        .left(cell)
                        .right(value.toString())
                        .build();
                result.add(newFix);
            }
        }

        // collect the remaining constant assign fix.
        Set<Cell> cells = assignMap.keySet();
        for (Cell cell : cells) {
            Fix newFix = fixBuilder.left(cell).right(assignMap.get(cell)).build();
            result.add(newFix);
        }
        return result;
    }
}
