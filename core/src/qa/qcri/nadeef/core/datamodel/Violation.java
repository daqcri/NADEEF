/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Violation class.
 * TODO: considering using ORM libraries like JOOQ.
 */
public class Violation {
    private String ruleId;
    private Set<ViolationRow> rows;

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
        rows = new HashSet();
    }

    /**
     * Gets the row collection.
     * @return collection row.
     */
    public Collection<ViolationRow> getRowCollection() {
        return rows;
    }

    /**
     * Adds a new violation for a rule.
     * @param tuple tuple.
     * @param column column.
     */
    public void addCell(Tuple tuple, Column column) {
        Preconditions.checkNotNull(tuple);
        Preconditions.checkNotNull(column);
        ViolationRow row = new ViolationRow(column, tuple.getTupleId(), tuple.getTupleId());
        rows.add(row);
    }
}
