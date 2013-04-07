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
    public FDRule(StringReader input) {
        parse(input);
        this.isSQLSupported = true;
    }

    public FDRule(String input) {
        this(new StringReader(input));
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
        for (Tuple tuple : tupleIterator) {

        }
        return null;
    }

    /**
     * Detect rule which runs in SQL.
     *
     * @return Violation set.
     */
    @Override
    public Violation detectInSQL(Connection conn) {
        return super.detectInSQL(conn);    //To change body of overridden methods use File | Settings | File Templates.
    }


}
