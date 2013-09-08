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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.sql.DBInstaller;
import qa.qcri.nadeef.test.NadeefTestBase;

/**
 * DBInstaller Test.
 */
@RunWith(Parameterized.class)
public class DBInstallerTest extends NadeefTestBase {

    public DBInstallerTest(String config) {
        super(config);
    }

    @Before
    public void setUp() {
        try {
            Bootstrap.start(testConfig);
            NadeefConfiguration.setTestMode(true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        Bootstrap.shutdown();
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
