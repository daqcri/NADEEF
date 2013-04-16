/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.tools;

import org.jooq.SQLDialect;

import java.sql.Connection;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.DBInstaller;

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
        try {
            if (DBInstaller.isInstalled(conn, NadeefConfiguration.getViolationTableName())) {
                DBInstaller.uninstall(conn, NadeefConfiguration.getViolationTableName());
            }
            DBInstaller.install(conn, NadeefConfiguration.getViolationTableName());
            Assert.assertTrue(
                    DBInstaller.isInstalled(
                            conn, NadeefConfiguration.getViolationTableName()
                    )
            );
            DBInstaller.uninstall(conn, NadeefConfiguration.getViolationTableName());
            Assert.assertFalse(
                DBInstaller.isInstalled(
                    conn,
                    NadeefConfiguration.getViolationTableName()
                )
            );
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
            ex.printStackTrace();
        }
    }
}
