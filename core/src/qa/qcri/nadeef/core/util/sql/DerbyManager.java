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

package qa.qcri.nadeef.core.util.sql;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

/**
 * Database manager for Apache Derby database.
 */
public class DerbyManager implements ISQLDialectManager {

    public static STGroupFile groupFile =
        new STGroupFile("qa/qcri/nadeef/core/util/sql/template/DerbyTemplate.stg", '$', '$');

    /**
     * {@inheritDoc}
     */
    @Override
    public String createViolationTable(String violationTableName) {
        ST st = groupFile.getInstanceOf("InstallViolationTable");
        st.add("violationTableName", violationTableName.toUpperCase());
        return st.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createRepairTable(String repairTableName) {
        ST st = groupFile.getInstanceOf("InstallRepairTable");
        st.add("repairTableName", repairTableName.toUpperCase());
        return st.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createAuditTable(String auditTableName) {
        ST st = groupFile.getInstanceOf("InstallAuditTable");
        st.add("auditTableName", auditTableName.toUpperCase());
        return st.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String dropTable(String tableName) {
        return "DROP TABLE " + tableName.toUpperCase();
    }
}
