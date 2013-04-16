/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test;

import qa.qcri.nadeef.core.datamodel.CleanPlan;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;

/**
 * Factory class to get TestData.
 */
public class TestDataRepository {

    public static String getCSVFilename() {
        return "test\\src\\qa\\qcri\\nadeef\\test\\input\\dumptest.csv";
    }

    public static String getFDFileName() {
        return "test\\src\\qa\\qcri\\nadeef\\test\\input\\CleanPlan1.json";

    }

    public static CleanPlan getFDCleanPlan() throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getFDFileName()));
    }
}
