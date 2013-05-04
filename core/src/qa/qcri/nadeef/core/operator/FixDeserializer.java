/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.util.DBConnectionFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

/**
 * Deserialize the fix data from database.
 */
public class FixDeserializer extends Operator<Rule, Collection<Fix>> {
    @Override
    public Collection<Fix> execute(Rule rule) throws Exception {
        Connection conn = DBConnectionFactory.createNadeefConnection();
        Collection<Fix> result = null;
        try {
            Statement stat = conn.createStatement();
            ResultSet resultSet = stat.executeQuery(
                "SELECT * FROM " +
                    NadeefConfiguration.getRepairTableName() +
                    " WHERE RID = " +
                    rule.getId() +
                    " ORDER BY vid"
            );

            // result = Fixes.fromQuery(resultSet);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return result;
    }
}
