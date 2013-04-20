/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test;

import qa.qcri.nadeef.core.datamodel.CleanPlan;

import java.io.*;
import java.sql.SQLException;

/**
 * Factory class to get TestData.
 */
public class TestDataRepository {

    public static File getDumpTestCSVFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*dumptest.csv";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getFDTestFile1() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan1.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getFDTestFile2() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan2.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static CleanPlan getFDCleanPlan()
            throws
                IOException,
                ClassNotFoundException,
                SQLException,
                InstantiationException,
                IllegalAccessException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getFDTestFile1()));
    }

    public static CleanPlan getFDCleanPlan2()
            throws
            IOException,
            ClassNotFoundException,
            SQLException,
            InstantiationException,
            IllegalAccessException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getFDTestFile2()));
    }

}
