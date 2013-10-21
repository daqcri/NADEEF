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
import qa.qcri.nadeef.core.util.Fixes;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;
import qa.qcri.nadeef.core.util.sql.SQLDialectBase;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;
import qa.qcri.nadeef.tools.sql.SQLDialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

/**
 * Imports the fix data from database.
 *
 */
class FixImport extends Operator<DBConfig, Collection<Fix>> {
    @Override
    public Collection<Fix> execute(DBConfig dbConfig) throws Exception {
        Connection conn = null;
        Statement stat = null;

        ResultSet resultSet = null;
        SQLDialect dialect = dbConfig.getDialect();
        SQLDialectBase dialectBase =
            SQLDialectBase.createDialectBaseInstance(dialect);
        Collection<Fix> result = null;
        try {
            conn = DBConnectionPool.createConnection(dbConfig);
            stat = conn.createStatement();
            String sql =
                dialectBase.selectAll(NadeefConfiguration.getRepairTableName());
            resultSet = stat.executeQuery(sql);
            result = Fixes.fromQuery(resultSet);
            Tracer.putStatsEntry(Tracer.StatType.FixImport, result.size());
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (stat != null) {
                stat.close();
            }

            if (conn != null) {
                conn.close();
            }
        }
        return result;
    }
}
