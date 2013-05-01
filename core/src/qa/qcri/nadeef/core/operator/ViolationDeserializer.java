/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Violations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

/**
 * Deserializing violations from violation table.
 */
public class ViolationDeserializer extends Operator<Rule, Collection<Violation>> {

    /**
     * Execute the operator.
     *
     * @param rule input rule.
     * @return output violations from database.
     */
    @Override
    public Collection<Violation> execute(Rule rule) throws Exception {
        Connection conn = DBConnectionFactory.createNadeefConnection();
        Collection<Violation> result = null;
        try {
            Statement stat = conn.createStatement();
            ResultSet resultSet = stat.executeQuery(
                "SELECT * FROM " +
                    NadeefConfiguration.getViolationTableName() +
                    " WHERE RID = '" +
                    rule.getId() +
                    "' ORDER BY vid"
            );

            result = Violations.fromQuery(resultSet);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return result;
    }
}
