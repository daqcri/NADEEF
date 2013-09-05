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

package qa.qcri.nadeef.test.tools;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.sql.DBInstaller;

import java.io.File;

/**
 * DBInstaller Test.
 */
public class DBInstallerTest {
    private static String testConfig1 =
        "test*src*qa*qcri*nadeef*test*input*config*derbyConfig.conf".replace(
            '*',
            File.separatorChar
        );

    @BeforeClass
    public static void setUp() {
        try {
            Bootstrap.start(testConfig1);
            NadeefConfiguration.setTestMode(true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void installerTest() {
        String violationTableName = NadeefConfiguration.getViolationTableName();
        String repairTableName = NadeefConfiguration.getRepairTableName();
        String auditTableName = NadeefConfiguration.getAuditTableName();
        try {
            if (DBInstaller.isInstalled(violationTableName)) {
                DBInstaller.uninstall(violationTableName);
            }

            if (DBInstaller.isInstalled(repairTableName)) {
                DBInstaller.uninstall(repairTableName);
            }

            if (DBInstaller.isInstalled(auditTableName)) {
                DBInstaller.uninstall(auditTableName);
            }

            DBInstaller.install(violationTableName, repairTableName, auditTableName);
            Assert.assertTrue(DBInstaller.isInstalled(violationTableName));
            DBInstaller.uninstall(violationTableName);
            DBInstaller.uninstall(repairTableName);

            Assert.assertFalse(DBInstaller.isInstalled(violationTableName));
            Assert.assertFalse(DBInstaller.isInstalled(repairTableName));
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
            ex.printStackTrace();
        }
    }
}
