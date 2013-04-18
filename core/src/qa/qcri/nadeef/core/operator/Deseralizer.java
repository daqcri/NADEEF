/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.base.Preconditions;
import org.jooq.SQLDialect;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.DBMetaDataTool;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.*;
import java.util.*;

/**
 * Deseralizer generates tuples for the rule. It also does the optimization
 * based on the input rule hints.
 */
public class Deseralizer<T> extends Operator<Rule, T> {
    private DBConfig dbConfig;

    /**
     * Constructor.
     * @param plan Clean plan.
     */
    public Deseralizer(CleanPlan plan) {
        super(plan);
        dbConfig = plan.getSourceDBConfig();
    }

    /**
     * Execute the operator.
     *
     * @param rule input rule.
     * @return output object.
     */
    @Override
    public T execute(Rule rule)
            throws
                ClassNotFoundException,
                SQLException,
                InstantiationException,
                IllegalAccessException {
        Preconditions.checkNotNull(rule);

        List<TupleCollection> result = new ArrayList<TupleCollection>();
        List<String> tableNames = rule.getTableNames();
        if (tableNames.size() == 2) {
            TupleCollectionPair pair =
                new TupleCollectionPair(
                    new TupleCollection(tableNames.get(0), dbConfig),
                    new TupleCollection(tableNames.get(1), dbConfig)
                );
            return (T)pair;
        }

        return (T)(new TupleCollection(tableNames.get(0), dbConfig));
    }
}
