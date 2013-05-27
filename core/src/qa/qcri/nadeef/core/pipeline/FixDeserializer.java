/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Fixes;
import qa.qcri.nadeef.tools.Tracer;

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
        Connection conn = DBConnectionFactory.getNadeefConnection();
        Collection<Fix> result = null;
        try {
            Statement stat = conn.createStatement();
            ResultSet resultSet =
                stat.executeQuery(
                    "select r.* from " +
                    NadeefConfiguration.getRepairTableName() +
                    " r where r.vid in (select vid from " +
                    NadeefConfiguration.getViolationTableName() +
                    " where rid = '" +
                    rule.getId() +
                    "') order by r.vid"
                );


            result = Fixes.fromQuery(resultSet);
            Tracer.addStatEntry(Tracer.StatType.FixDeserialize, result.size());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return result;
    }
}
