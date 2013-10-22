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

package qa.qcri.nadeef.console;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import jline.console.ConsoleReader;
import jline.console.completer.*;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.pipeline.UpdateExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.sql.DBInstaller;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * User interactive console.
 *
 */
public class Console {

    //<editor-fold desc="Private fields">
    private static final String logo =
        "   _  __        __        _____\n" +
        "  / |/ /__ ____/ /__ ___ /   _/\n" +
        " /    / _ `/ _  / -_) -_)   _/\n" +
        "/_/|_/\\_,_/\\_,_/\\__/\\__/ __/\n" +
        "Data Cleaning solution (Build " + System.getenv("BuildVersion")  +
        ", using Java " + System.getProperty("java.version") + ").\n" +
        "Copyright (C) Qatar Computing Research Institute, 2013 - Present (http://da.qcri.org).";

    private static final String helpInfo = "Type 'help' to see what commands we have.";

    private static final String prompt = ":> ";

    private static final String[] commands =
        { "load", "run", "repair", "detect", "help", "set", "exit"};
    private static ConsoleReader console;
    private static List<CleanPlan> cleanPlans;
    private static List<CleanExecutor> executors = Lists.newArrayList();
    private static Tracer tracer = Tracer.getTracer(Console.class);
    private static Process derbyProcess;
    private static final int DERBY_PORT = 1527;

    //</editor-fold>

    //<editor-fold desc="Detect Thread class">
    /**
     * Detect thread.
     */
    private static class DetectRunnable implements Runnable {
        private CleanExecutor executor;
        public DetectRunnable(CleanExecutor executor) {
            this.executor = executor;
        }

        @Override
        public void run() {
            executor.detect();
        }
    }
    //</editor-fold>

    //<editor-fold desc="Repair Thread class">

    /**
     * Repair thread class.
     */
    private static class RepairRunnable implements Runnable {
        private CleanExecutor executor;
        public RepairRunnable(CleanExecutor cleanExecutor) {
            this.executor = cleanExecutor;
        }

        @Override
        public void run() {
            executor.repair();
        }
    }

    //</editor-fold>

    //<editor-fold desc="Clean Thread class">

    /**
     * Repair thread class.
     */
    private static class CleanRunnable implements Runnable {
        private CleanExecutor executor;
        public CleanRunnable(CleanExecutor cleanExecutor) {
            this.executor = cleanExecutor;
        }

        @Override
        public void run() {
            executor.detect();
            executor.repair();
        }
    }

    //</editor-fold>
    /**
     * Start of Console.
     * @param args user input.
     */
    public static void main(String[] args) {
        try {
            // check Java version
            String jdkVersion = System.getProperty("java.version");
            if (!jdkVersion.startsWith("1.7")) {
                System.err.println("NADEEF console needs to be used with JDK 1.7+.");
                System.exit(1);
            }

            // start derby db.
            System.out.print("Start embedded database...");
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    if (derbyProcess != null)  {
                        derbyProcess.destroy();
                    }
                }
            });
            derbyProcess =
                Runtime.getRuntime().exec("java -d64 -jar out/bin/derbyrun.jar server start");
            if (!CommonTools.waitForService(DERBY_PORT)) {
                System.out.println("FAILED");
                System.exit(1);
            }

            System.out.println("OK");
            // bootstrap Nadeef.
            Stopwatch stopwatch = new Stopwatch().start();
            Bootstrap.start();

            console = new ConsoleReader();
            Tracer.setConsole(new ConsoleReaderAdaptor(console));
            List<Completer> loadCompleter =
                Arrays.asList(
                    new StringsCompleter(commands),
                    new FileNameCompleter(),
                    new NullCompleter()
                );
            console.addCompleter(new ArgumentCompleter(loadCompleter));

            console.clearScreen();
            console.println(logo);
            console.println();
            console.println(helpInfo);
            console.println();
            console.drawLine();
            console.setPrompt(prompt);
            console.println(
                "Your NADEEF started in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms."
            );

            String line;
            while ((line = console.readLine()) != null) {
                line = line.trim();
                String[] tokens = line.split(" ");

                if (tokens.length == 0) {
                    continue;
                }

                // clear the statistics for every run.
                Tracer.clearStats();
                try {
                    if (tokens[0].equalsIgnoreCase("exit")) {
                        break;
                    } else if (tokens[0].equalsIgnoreCase("load")) {
                        load(line);
                    } else if (tokens[0].equalsIgnoreCase("list")) {
                        list();
                    } else if (tokens[0].equalsIgnoreCase("help")) {
                        printHelp();
                    } else if (tokens[0].equalsIgnoreCase("detect")) {
                        detect(line);
                    } else if (tokens[0].equalsIgnoreCase("repair")) {
                        repair(line);
                    } else if (tokens[0].equalsIgnoreCase("run")) {
                        run(line);
                    } else if (tokens[0].equalsIgnoreCase("set")) {
                        set(line);
                    } else if (!Strings.isNullOrEmpty(tokens[0])) {
                        console.println("I don't know this command.");
                    }
                } catch (Exception ex) {
                    console.println(
                        "Oops, something is wrong. Please check the log in the output dir."
                    );

                    tracer.err("", ex);
                }
            }
        } catch (Exception ex) {
            try {
                tracer.err("Bootstrap failed", ex);
            } catch (Exception ignore) {}
        } finally {
            Bootstrap.shutdown();
        }

        System.exit(0);
    }

    private static void load(String cmdLine) throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        String[] splits = cmdLine.split("\\s");
        if (splits.length != 2) {
            console.println("Invalid load command. Run load <Nadeef config file>.");
            return;
        }
        String fileName = splits[1];
        File file = CommonTools.getFile(fileName);

        // shutdown existing executors
        for (CleanExecutor executor : executors) {
            executor.shutdown();
        }
        executors.clear();
        try {
            DBConfig dbConfig = NadeefConfiguration.getDbConfig();
            cleanPlans =
                CleanPlan.createCleanPlanFromJSON(
                    new FileReader(file),
                    dbConfig
                );
            for (CleanPlan cleanPlan : cleanPlans) {
                executors.add(new CleanExecutor(cleanPlan, dbConfig));
            }
        } catch (Exception ex) {
            tracer.err("Loading CleanPlan failed.", ex);
            return;
        }
        console.println(
            cleanPlans.size()
                + " rules loaded in "
                + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms."
        );
    }

    private static void list() throws IOException {
        if (cleanPlans == null) {
            console.println("There is no rule loaded.");
            return;
        }

        console.println("There are " + cleanPlans.size() + " rules loaded.");
        for (int i = 0; i < cleanPlans.size(); i ++) {
            console.println("\t" + i + ": " + cleanPlans.get(i).getRule().getRuleName());
        }
    }

    private static void detect(String cmd) throws IOException, InterruptedException {
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            console.println("Wrong detect command. Run detect [id number] instead.");
            return;
        }

        if (executors == null || executors.size() == 0) {
            console.println("There is no rule loaded.");
            return;
        }

        int index = -1;
        if (tokens.length == 2) {
            index = Integer.valueOf(tokens[1]);
            if (index < 0 || index >= cleanPlans.size()) {
                console.println("Out of index.");
                return;
            }
        }

        for (int i = 0; i < executors.size(); i ++) {
            if (index != -1 && i != index) {
                continue;
            }

            CleanExecutor executor = executors.get(i);
            Thread thread = new Thread(new DetectRunnable(executor));
            thread.start();

            do {
                Thread.sleep(1000);
                double percentage = executor.getDetectProgress();
                printProgress(percentage, "DETECT");
            } while (thread.isAlive());

            // print out the final result.
            String ruleName = executor.getCleanPlan().getRule().getRuleName();
            double percentage = executor.getDetectProgress();
            printProgress(percentage, "DETECT");
            console.println();
            console.flush();
            Tracer.printDetectSummary(ruleName);
        }
    }

    private static void repair(String cmd) throws IOException, InterruptedException {
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            console.println("Wrong repair command. Run repair [id number] instead.");
        }

        if (executors == null || executors.size() == 0) {
            console.println("There is no rule loaded.");
            return;
        }

        int index = -1;
        if (tokens.length == 2) {
            index = Integer.valueOf(tokens[1]);
            if (index < 0 && index >= cleanPlans.size()) {
                console.println("Out of index.");
                return;
            }
        }

        for (int i = 0; i < executors.size(); i ++) {
            if (index != -1 && index != i) {
                continue;
            }

            CleanExecutor executor = executors.get(i);
            Thread thread = new Thread(new RepairRunnable(executor));
            thread.start();

            do {
                Thread.sleep(1000);
                double percentage = executor.getRepairProgress();
                printProgress(percentage, "REPAIR");
            } while (thread.isAlive());

            // print out the final result.
            String ruleName = executor.getCleanPlan().getRule().getRuleName();
            double percentage = executor.getRepairProgress();
            printProgress(percentage, "REPAIR");
            console.println();
            console.flush();
            Tracer.printRepairSummary(ruleName);
        }
    }

    private static void run(String cmd)
        throws IOException, InterruptedException {
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            console.println("Wrong repair command. Run repair [id number] instead.");
        }

        if (executors == null || executors.size() == 0) {
            console.println("There is no rule loaded.");
            return;
        }

        int index = -1;
        if (tokens.length == 2) {
            index = Integer.valueOf(tokens[1]);
            if (index < 0 && index >= cleanPlans.size()) {
                console.println("Out of index.");
                return;
            }
        }

        // clean the database first before start another iterations of clean.
        try {
            DBInstaller.cleanExecutionDB(NadeefConfiguration.getDbConfig());
        } catch (Exception ex) {
            tracer.err("Clean existing data failed.", ex);
        }

        DBConfig sourceDbConfig = executors.get(0).getConnectionPool().getSourceDBConfig();
        UpdateExecutor updateExecutor = new UpdateExecutor(sourceDbConfig);
        int updatedCell = 0;
        int maxIterationNumber = 0;
        do {
            for (int i = 0; i < executors.size(); i ++) {
                if (index != -1 && index != i) {
                    continue;
                }

                CleanExecutor executor = executors.get(i);
                Thread thread = new Thread(new CleanRunnable(executor));
                thread.start();

                do {
                    Thread.sleep(1000);
                    double percentage = executor.getRepairProgress();
                    printProgress(percentage, "CLEAN");
                } while (thread.isAlive());

                // print out the final result.
                double percentage = executor.getRepairProgress();
                printProgress(percentage, "CLEAN");
                console.println();
                console.flush();
            }

            // do the final holistic update
            updateExecutor.run();
            updatedCell = updateExecutor.getUpdateCellCount();
            maxIterationNumber ++;
        } while (
            updatedCell != 0 &&
            maxIterationNumber <= NadeefConfiguration.getMaxIterationNumber()
        );

        // Print overall statistics
        for (int i = 0; i < executors.size(); i ++) {
            if (index != -1 && index != i) {
                continue;
            }
            CleanExecutor executor = executors.get(i);
            String ruleName = executor.getCleanPlan().getRule().getRuleName();
            Tracer.printDetectSummary(ruleName);
            Tracer.printRepairSummary(ruleName);
        }
        console.println();
        Tracer.printUpdateSummary();
    }

    private static void set(String cmd) throws IOException {
        String[] tokens = cmd.split("\\s");
        if (tokens[1].equalsIgnoreCase("verbose")) {
            boolean mode = !Tracer.isVerboseOn();
            console.println("set verbose " + (mode ? "on" : "off"));
            Tracer.setVerbose(mode);
        }

        if (tokens[1].equalsIgnoreCase("info")) {
            boolean mode = !Tracer.isInfoOn();
            console.println("set info " + (mode ? "on" : "off"));
            Tracer.setInfo(mode);
        }

        if (tokens[1].equalsIgnoreCase("alwaysCompile")) {
            boolean mode = !NadeefConfiguration.getAlwaysCompile();
            console.println("set alwaysCompile " + (mode ? "on" : "off"));
            NadeefConfiguration.setAlwaysCompile(mode);
        }
    }

    private static void printHelp() throws IOException {
        String help =
                " |Nadeef console usage:\n" +
                " |----------------------------------\n" +
                " |help : Print out this help information.\n" +
                " |\n" +
                " |load <input CleanPlan file> :\n" +
                " |    load a NADEEF clean plan.\n" +
                " |\n" +
                " |detect [rule id] :\n" +
                " |    start the violation detection with a given rule id number.\n" +
                " |\n" +
                " |list : \n" +
                " |    list available rules.\n" +
                " |\n" +
                " |repair [rule id] :\n" +
                " |    repair the data source with a given rule id number.\n" +
                " |\n" +
                " |run [rule id]:\n" +
                " |    run both detect and repair with a given rule id number. \n" +
                " |\n" +
                " |schema [table name]: \n" +
                " |    list the table schema from the data source. \n" +
                " |\n" +
                " |exit :\n" +
                " |    exit the console.\n";
        console.println(help);
    }


    //<editor-fold desc="Private helpers">
    private static void printProgress(double percentage, String title) throws IOException {
        console.redrawLine();
        int ne = (int)Math.round(percentage * 50.f);
        StringBuilder stringBuilder = new StringBuilder(512);
        stringBuilder.append('[').append(title).append("][");
        for (int i = 0; i < ne; i ++) {
            stringBuilder.append("=");
        }

        if (ne < 50) {
            stringBuilder.append(">");
            for (int i = 0; i < 50 - ne; i ++) {
                stringBuilder.append(" ");
            }
        }
        stringBuilder.append("]");
        stringBuilder.append(Math.round(Double.valueOf(percentage) * 100));
        stringBuilder.append(" %");
        console.print(stringBuilder.toString());
        console.flush();
    }
    //</editor-fold>
}
