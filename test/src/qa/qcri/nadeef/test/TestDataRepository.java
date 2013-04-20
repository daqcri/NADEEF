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

    public static String getCSVFilename() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*dumptest.csv";
        return filePath.replace('*', File.separatorChar);
    }

    public static String getFDFileName() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan1.json";
        return filePath.replace('*', File.separatorChar);
    }

    public static CleanPlan getFDCleanPlan() throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getFDFileName()));
    }
}
