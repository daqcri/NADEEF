/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test;

import qa.qcri.nadeef.core.datamodel.CleanPlan;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringReader;

/**
 * Factory class to get TestData.
 */
public class TestDataRepository {

    public static String getFDFileName() {
        return "test\\src\\qa\\qcri\\nadeef\\test\\input\\CleanPlan1.json";

    }

    public static CleanPlan getFDCleanPlan() throws FileNotFoundException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getFDFileName()));
    }
}
