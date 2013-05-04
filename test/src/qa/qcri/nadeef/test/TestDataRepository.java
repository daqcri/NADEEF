/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test;

import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.exception.InvalidCleanPlanException;
import qa.qcri.nadeef.core.exception.InvalidRuleException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Factory class to get TestData.
 */
public class TestDataRepository {

    public static File getDumpTestCSVFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*dumptest.csv";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getTestFile1() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan1.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getTestFile2() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan2.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getTestFile3() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan3.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getTestFile4() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan4.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getTestFile5() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan5.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getViolationTestData1() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*violation1.csv";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getFixTestData1() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*Fix1.csv";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getLocationData1() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*locations.csv";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static CleanPlan getCleanPlan()
        throws
            InvalidRuleException,
            FileNotFoundException,
            InvalidCleanPlanException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile1()));
    }

    public static CleanPlan getCleanPlan2()
        throws
        InvalidRuleException,
        FileNotFoundException,
        InvalidCleanPlanException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile2()));
    }

    public static CleanPlan getCleanPlan3()
        throws
        InvalidRuleException,
        FileNotFoundException,
        InvalidCleanPlanException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile3()));
    }

    public static CleanPlan getCleanPlan4()
        throws
        InvalidRuleException,
        FileNotFoundException,
        InvalidCleanPlanException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile4()));
    }

    public static CleanPlan getCleanPlan5()
        throws
        InvalidRuleException,
        FileNotFoundException,
        InvalidCleanPlanException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile5()));
    }
}
