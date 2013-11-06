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

package qa.qcri.nadeef.test;

import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;

import java.io.File;
import java.io.FileReader;
import java.util.List;

/**
 * Factory class to get TestData.
 */
public class TestDataRepository {
    public static String DerbyConfig =
        "test*src*qa*qcri*nadeef*test*input*config*derbyConfig.conf".replace(
                '*', File.separatorChar);

    public static String PostgresConfig =
        "test*src*qa*qcri*nadeef*test*input*config*postgresConfig.conf".replace(
                '*', File.separatorChar);

    public static String MySQLConfig =
            "test*src*qa*qcri*nadeef*test*input*config*mysqlConfig.conf".replace(
                '*', File.separatorChar);

    public static File getDumpTestCSVFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*dumptest.csv";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getPairCleanPlanFile1() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*PairTableCleanPlan1.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getFailurePlanFile1() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*FailurePlan1.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getFailurePlanFile2() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*FailurePlan2.json";
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

    public static File getCleanPlan6File() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan6.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getCleanPlan7File() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan7.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getCleanPlan8File() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan8.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getCleanPlan9File() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*CleanPlan9.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getHolisticPlan1File() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*HolisticTestPlan1.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getStressPlan10kFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*StressPlan10k.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getStressPlan30kFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*StressPlan30k.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getStressPlan40kFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*StressPlan40k.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getStressPlan80kFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*StressPlan80k.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getStressPlan90kFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*StressPlan90k.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getStressPlan100kFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*StressPlan100k.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static List<CleanPlan> getHolisticTestPlan1()
        throws Exception {
        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getHolisticPlan1File()), NadeefConfiguration.getDbConfig());
    }

    public static File getDCTestFile(){
        final String filePath = "test*src*qa*qcri*nadeef*test*input*DCCleanPlan3.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getConstantDCTestFile(){
        final String filePath = "test*src*qa*qcri*nadeef*test*input*DCCleanPlan2.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getSingleTupleDCTestFile(){
        final String filePath = "test*src*qa*qcri*nadeef*test*input*DCCleanPlan1.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getFloatDCTestFile(){
        final String filePath = "test*src*qa*qcri*nadeef*test*input*DCCleanPlan4.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getDCGeneratedFile(){
        final String filePath = "test*src*qa*qcri*nadeef*test*input*DCCleanPlan5.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getIncCleanPlanFile() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*IncCleanPlan1.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static File getIncCleanPlanFile2() {
        final String filePath = "test*src*qa*qcri*nadeef*test*input*IncCleanPlan2.json";
        return new File(filePath.replace('*', File.separatorChar));
    }

    public static CleanPlan getCleanPlan()
        throws Exception {
        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getTestFile1()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getCleanPlan2()
        throws Exception {
        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getTestFile2()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getCleanPlan3()
        throws Exception {
        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getTestFile3()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getCleanPlan4()
        throws Exception {
        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getTestFile4()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static List<CleanPlan> getCleanPlan5()
        throws Exception {

        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getTestFile5()), NadeefConfiguration.getDbConfig());
    }

    public static CleanPlan getCleanPlan6()
        throws Exception {

            return CleanPlan.createCleanPlanFromJSON(
                new FileReader(getCleanPlan6File()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getCleanPlan7()
        throws Exception {

        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getCleanPlan7File()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getCleanPlan8()
        throws Exception {

        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getCleanPlan8File()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getCleanPlan9()
        throws Exception {

        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getCleanPlan9File()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getStressPlan10k() throws Exception {
        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getStressPlan10kFile()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getStressPlan30k()
        throws Exception {

        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getStressPlan30kFile()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getStressPlan40k()
        throws Exception {

        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getStressPlan40kFile()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getStressPlan80k()
        throws Exception {

        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getStressPlan80kFile()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getStressPlan90k()
        throws Exception {

        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getStressPlan90kFile()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getPairCleanPlan1()
        throws Exception {

        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getPairCleanPlanFile1()), NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getDCTestPlan()
        throws Exception {

      return CleanPlan.createCleanPlanFromJSON(
          new FileReader(getDCTestFile()),
          NadeefConfiguration.getDbConfig()
      ).get(0);
    }

    public static CleanPlan getConstantDCTestPlan()
        throws Exception {

      return CleanPlan.createCleanPlanFromJSON(
          new FileReader(getConstantDCTestFile()),
          NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getSingleTupleDCTestPlan()
        throws Exception {

      return CleanPlan.createCleanPlanFromJSON(
          new FileReader(getSingleTupleDCTestFile()),
          NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getFloatDCTestPlan() throws Exception {
      return CleanPlan.createCleanPlanFromJSON(
          new FileReader(getFloatDCTestFile()),
          NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getDCGeneratedCleanPlan() throws Exception {
        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getDCGeneratedFile()),
            NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getIncCleanPlan1() throws Exception {
        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getIncCleanPlanFile()),
            NadeefConfiguration.getDbConfig()).get(0);
    }

    public static CleanPlan getIncCleanPlan2() throws Exception {
        return CleanPlan.createCleanPlanFromJSON(
            new FileReader(getIncCleanPlanFile2()),
            NadeefConfiguration.getDbConfig()).get(0);
    }
}

