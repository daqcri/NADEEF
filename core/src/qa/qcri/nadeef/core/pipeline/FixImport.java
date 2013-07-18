/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
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
 * Imports the fix data from database.
 */
class FixImport extends Operator<Rule, Collection<Fix>> {
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
                    rule.getRuleName() +
                    "') order by r.vid"
                );


            result = Fixes.fromQuery(resultSet);
            Tracer.putStatsEntry(Tracer.StatType.FixDeserialize, result.size());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return result;
    }
}
