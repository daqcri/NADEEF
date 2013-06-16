/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.SQLTable;
import qa.qcri.nadeef.core.datamodel.Table;
import qa.qcri.nadeef.tools.DBConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * SourceDeserializer generates tuples for the rule. It also does the optimization
 * based on the input rule hints.
 */
public class SourceDeserializer extends Operator<Rule, Collection<Table>> {
    private DBConfig dbConfig;

    /**
     * Constructor.
     * @param plan Clean plan.
     */
    public SourceDeserializer(CleanPlan plan) {
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
    @SuppressWarnings("unchecked")
    public Collection<Table> execute(Rule rule) {
        Preconditions.checkNotNull(rule);

        List<String> tableNames = (List<String>)rule.getTableNames();
        List<Table> collections = new ArrayList<Table>();
        if (tableNames.size() == 2) {
            collections.add(new SQLTable(tableNames.get(0), dbConfig));
            collections.add(new SQLTable(tableNames.get(1), dbConfig));
        } else {
            collections.add(new SQLTable(tableNames.get(0), dbConfig));
        }

        return collections;
    }
}
