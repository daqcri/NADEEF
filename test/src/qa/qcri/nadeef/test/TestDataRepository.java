/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test;

import qa.qcri.nadeef.core.datamodel.CleanPlan;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

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

    public static CleanPlan getCleanPlan()
        throws
        IOException,
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException,
        NoSuchMethodException,
        InvocationTargetException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile1()));
    }

    public static CleanPlan getCleanPlan2()
        throws
        IOException,
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException,
        NoSuchMethodException,
        InvocationTargetException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile2()));
    }

    public static CleanPlan getCleanPlan3()
        throws
        IOException,
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException,
        NoSuchMethodException,
        InvocationTargetException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile3()));
    }

    public static CleanPlan getCleanPlan4()
        throws
        IOException,
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException,
        NoSuchMethodException,
        InvocationTargetException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile4()));
    }

    public static CleanPlan getCleanPlan5()
        throws
        IOException,
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException,
        NoSuchMethodException,
        InvocationTargetException {
        return CleanPlan.createCleanPlanFromJSON(new FileReader(getTestFile5()));
    }
}
