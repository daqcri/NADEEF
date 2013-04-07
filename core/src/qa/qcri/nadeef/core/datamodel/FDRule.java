/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.io.StringReader;
import java.sql.Connection;

/**
 * Function Dependency (FD) Rule.
 */
public class FDRule extends TextRule {
    private String[] lhs;
    private String[] rhs;

    /**
     * Constructor.
     * @param input Input stream.
     */
    public FDRule(StringReader input, String[] lhs, String[] rhs) {
        parse(input);
        isSQLSupported = true;
    }

    @Override
    public void parse(StringReader input) {
    }

    /**
     * Detect rule with multiple tuples.
     * @param tupleIterator tuple iterator.
     * @return Violation set.
     */
    @Override
    public Violation detect(Iterable<Tuple> tupleIterator) {
        return null;
    }

    /**
     * Detect rule which runs in SQL.
     *
     * @return Violation set.
     */
    @Override
    public Violation detectInSQL(Connection conn) {
        return null;
    }
}
