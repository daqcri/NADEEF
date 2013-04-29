/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.collect.*;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Fix;

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
        HashMap<Column, HashSet<Cell>> clusterMap = Maps.newHashMap();
        HashMap<Cell, String> assignMap = Maps.newHashMap();

        // Clustering all the fixes.
        for (Fix fix : fixes) {
            Cell leftCell = fix.getLeft();
            Column leftColumn = leftCell.getColumn();

            if (fix.isConstantAssign()) {
                // TODO: do a statistic on the assign count.
                assignMap.put(leftCell, fix.getRightValue());
                continue;
            }

            Cell rightCell = fix.getRight();
            Column rightColumn = rightCell.getColumn();

            if (assignMap.containsKey(leftColumn)) {
                assignMap.remove(leftColumn);
            }

            if (assignMap.containsKey(rightColumn)) {
                assignMap.remove(rightColumn);
            }

            HashSet<Cell> leftCluster = null;
            HashSet<Cell> rightCluster = null;

            // when the left column is already in a cluster
            if (clusterMap.containsKey(leftColumn)) {
                leftCluster = clusterMap.get(leftColumn);
                if (!leftCluster.contains(rightColumn)) {
                    // union of two cluster of cell sets.
                    if (clusterMap.containsKey(rightColumn)) {
                        rightCluster = clusterMap.get(rightColumn);
                        for (Cell cell : rightCluster) {
                            leftCluster.add(cell);
                            clusterMap.put(cell.getColumn(), leftCluster);
                        }

                        for (Cell cell : rightCluster) {
                            rightCluster.remove(cell);
                        }

                        clusters.remove(rightCluster);
                    } else {
                        clusterMap.put(rightColumn, leftCluster);
                    }
                }
            } else if (clusterMap.containsKey(rightColumn)) {
                // when the right column is already in the cluster
                rightCluster = clusterMap.get(rightColumn);
                if (!rightCluster.contains(leftColumn)) {
                    // union of two cluster of cell sets.
                    if (clusterMap.containsKey(leftColumn)) {
                        leftCluster = clusterMap.get(leftColumn);
                        for (Cell cell : leftCluster) {
                            rightCluster.add(cell);
                            clusterMap.put(cell.getColumn(), rightCluster);
                        }

                        for (Cell cell : leftCluster) {
                            leftCluster.remove(cell);
                        }

                        clusters.remove(leftCluster);
                    } else {
                        clusterMap.put(leftColumn, rightCluster);
                    }
                }
            } else {
                // both left and right are not in any of the cluster
                // create a new cluster of containing both.
                HashSet<Cell> cluster = Sets.newHashSet();
                cluster.add(leftCell);
                cluster.add(rightCell);
                clusterMap.put(leftColumn, cluster);
                clusterMap.put(rightColumn, cluster);
                clusters.add(cluster);
            }
        }

        // start to count each cluster and decide the final fix based on
        // percentage.
        List<Fix> result = Lists.newArrayList();
        Fix.Builder fixBuilder = new Fix.Builder();
        for (HashSet<Cell> cluster : clusters) {
            Multiset<Object> countSet = HashMultiset.create();
            for (Cell cell : cluster) {
                countSet.add(cell.getAttributeValue());
            }

            countSet = Multisets.copyHighestCountFirst(countSet);
            Object value = countSet.iterator().next();
            for (Cell cell : cluster) {
                Fix newFix = fixBuilder.left(cell).right(value.toString()).build();
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
