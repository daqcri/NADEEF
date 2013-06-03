/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;

/**
 * Violation class.
 */
public class Violation {
    public static final int UnknownId = -1;
    private String ruleId;
    private Set<Cell> cells;
    private Optional<Integer> vid;

    /**
     * Gets the rule Id of this violation.
     * @return rule Id.
     */
    public String getRuleId() {
        return this.ruleId;
    }

    /**
     * Constructor.
     * @param ruleId rule id.
     */
    public Violation(String ruleId) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(ruleId));
        this.ruleId = ruleId;
        cells = Sets.newHashSet();
        vid = Optional.absent();
    }

    /**
     * Constructor.
     * @param ruleId rule id.
     */
    public Violation(String ruleId, int vid) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(ruleId));
        this.ruleId = ruleId;
        cells = Sets.newHashSet();
        this.vid = Optional.of(vid);
    }

    /**
     * Gets the row collection.
     * @return collection row.
     */
    public Collection<Cell> getCells() {
        return cells;
    }

    public Cell getCell(String tableName, String columnName) {
        for (Cell cell : cells) {
            Column column = cell.getColumn();
            if (
                column.isFromTable(tableName) &&
                column.getColumnName().equalsIgnoreCase(columnName)
            ) {
                return cell;
            }
        }
        return null;
    }

    /**
     * Adds a new violated cell.
     * @param cell violated cell.
     */
    public void addCell(Cell cell) {
        Preconditions.checkNotNull(cell);
        cells.add(cell);
    }

    /**
     * Adds a whole <code>Tuple</code> as violated tuple.
     * @param tuple tuple.
     */
    public void addTuple(Tuple tuple) {
        Preconditions.checkNotNull(tuple);
        cells.addAll(tuple.getCells());
    }

    /**
     * Gets the violation id.
     * @return violation id, -1 is returned if no violation id exists.
     */
    public int getVid() {
        return vid.or(-1);
    }

    /**
     * Sets the vid.
     * @param vid vid.
     */
    public void setVid(int vid) {
        this.vid = Optional.of(vid);
    }
}
