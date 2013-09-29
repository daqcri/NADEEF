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

package qa.qcri.nadeef.web;

import com.google.common.base.Preconditions;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;
import qa.qcri.nadeef.tools.Tracer;
import qa.qcri.nadeef.tools.sql.SQLDialect;
import qa.qcri.nadeef.web.sql.DerbySQLDialect;
import qa.qcri.nadeef.web.sql.MySQLDialect;
import qa.qcri.nadeef.web.sql.PostgresSQLDialect;
import qa.qcri.nadeef.web.sql.SQLDialectBase;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DB Installer for Dashboard.
 */
class DBInstaller {
    private static Tracer tracer = Tracer.getTracer(DBInstaller.class);
    static SQLDialectBase dialectInstance;

    public static void install() throws Exception {
        Connection conn = null;
        Statement stat = null;
        try {
            conn = DBConnectionPool.createConnection(NadeefConfiguration.getDbConfig());
            stat = conn.createStatement();

            // TODO: do inject dep. for generic function
            SQLDialect dialect = NadeefConfiguration.getDbConfig().getDialect();
            switch (dialect) {
                case DERBYMEMORY:
                case DERBY:
                    dialectInstance = new DerbySQLDialect();
                    break;
                case POSTGRES:
                    dialectInstance = new PostgresSQLDialect();
                    break;
                case MYSQL:
                    dialectInstance = new MySQLDialect();
                    break;
            }

            Preconditions.checkNotNull(dialectInstance);

            // skip when those dbs are already existed
            DatabaseMetaData meta = conn.getMetaData();
            if (!meta.getTables(null, null, "RULE", null).next() &&
                !meta.getTables(null, null, "rule", null).next()) {
                stat.execute(dialectInstance.installRule());
            }

            if (!meta.getTables(null, null, "RULETYPE", null).next() &&
                !meta.getTables(null, null, "ruletype", null).next()) {
                stat.execute(dialectInstance.installRuleType());
                stat.execute("INSERT INTO RULETYPE VALUES (0, 'UDF', true)");
                stat.execute("INSERT INTO RULETYPE VALUES (1, 'FD', true)");
                stat.execute("INSERT INTO RULETYPE VALUES (2, 'CFD', true)");
            }
            conn.commit();
        } catch (SQLException ex) {
            tracer.err("Install Dashboard DB failed.", ex);
            System.exit(1);
        } finally {
            if (stat != null) {
                stat.close();
            }

            if (conn != null) {
                conn.close();
            }
        }

    }
}
