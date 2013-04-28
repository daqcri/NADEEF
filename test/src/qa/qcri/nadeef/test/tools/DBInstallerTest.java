/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.tools;

import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.tools.DBInstaller;

import java.sql.Connection;

/**
 * DBInstaller Test.
 */
public class DBInstallerTest {
    private static Connection conn;
    private static NadeefConfiguration configuration;

    private static final String url = "jdbc:postgresql://localhost/unittest";
    private static final String userName = "tester";
    private static final String password = "tester";

    @BeforeClass
    public static void setUp() {
        Bootstrap.Start();
        try {
            conn =
                DBConnectionFactory.createConnection(SQLDialect.POSTGRES, url, userName, password);
            conn.setAutoCommit(false);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
        NadeefConfiguration.setTestMode(true);
    }

    @Test
    public void installerTest() {
        String violationTableName = NadeefConfiguration.getViolationTableName();
        String repairTableName = NadeefConfiguration.getRepairTableName();
        try {
            if (DBInstaller.isInstalled(conn, violationTableName)) {
                DBInstaller.uninstall(conn, violationTableName);
            }

            if (DBInstaller.isInstalled(conn, repairTableName)) {
                DBInstaller.uninstall(conn, repairTableName);
            }

            DBInstaller.install(conn, violationTableName, repairTableName);
            Assert.assertTrue(DBInstaller.isInstalled(conn, violationTableName));
            DBInstaller.uninstall(conn, violationTableName);
            DBInstaller.uninstall(conn, repairTableName);

            Assert.assertFalse(DBInstaller.isInstalled(conn, violationTableName));
            Assert.assertFalse(DBInstaller.isInstalled(conn, repairTableName));
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
            ex.printStackTrace();
        }
    }
}
