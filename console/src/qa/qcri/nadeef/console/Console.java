/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
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
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.exception.InvalidCleanPlanException;
import qa.qcri.nadeef.core.exception.InvalidRuleException;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.DBMetaDataTool;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * User interactive console.
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
            "Copyright (C) Qatar Computing Research Institute, 2013 (http://da.qcri.org).";

    private static final String helpInfo = "Type 'help' to see what commands we have.";

    private static final String prompt = ":> ";

    private static final String[] commands =
        { "load", "run", "repair", "detect", "help", "set", "exit", "schema", "fd" };
    private static ConsoleReader console;
    private static List<CleanPlan> cleanPlans;
    private static List<CleanExecutor> executors = Lists.newArrayList();
    private static Tracer tracer = Tracer.getTracer(Console.class);

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

    //<editor-fold desc="Clean thread">
    /**
     * Clean thread class.
     */
    private static class CleanRunnable implements Runnable {
        private CleanExecutor executor;

        public CleanRunnable(CleanExecutor cleanExecutor) {
            this.executor = cleanExecutor;
        }

        @Override
        public void run() {
            executor.run();
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
                // TODO: move inside the executor.
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
                    } else if (tokens[0].equalsIgnoreCase("fd")) {
                        fd(line);
                    } else if (tokens[0].equalsIgnoreCase("schema")) {
                        schema(line);
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

    //<editor-fold desc="Schema command">
    private static void schema(String cmdLine) throws Exception {
        String[] splits = cmdLine.split("\\s");
        if (splits.length != 2) {
            console.println(
                "Invalid schema command. Please try to use schema [table name]."
            );
            return;
        }

        if (cleanPlans == null) {
            console.println(
                "There is no CleanPlan loaded."
            );
            return;
        }

        String tableName = splits[1];
        if (!DBMetaDataTool.isTableExist(tableName)) {
            console.println("Unknown table names.");
            return;
        }

        Schema schema = DBMetaDataTool.getSchema(tableName);
        List<Column> columns = schema.getColumns();
        for (Column column : columns) {
            if (column.getColumnName().equals("tid")) {
                continue;
            }
            console.println(String.format("\t%s", column.getColumnName()));
        }
    }
    //</editor-fold>

    // TODO: remove FD specification, and make it generic
    private static void fd(String cmdLine) throws Exception {
        if (cleanPlans.size() == 0) {
            console.println("There is no rule loaded.");
            return;
        }

        int index = cmdLine.indexOf("fd") + 2;
        String value = cmdLine.substring(index);
        RuleBuilder ruleBuilder = NadeefConfiguration.tryGetRuleBuilder("fd");
        String tableName = (String) cleanPlans.get(0).getRule().getTableNames().get(0);
        Schema schema = DBMetaDataTool.getSchema(tableName);
        Collection<Rule> rules =
            ruleBuilder
                .name("UserRule" + CommonTools.toHashCode(value))
                .table(tableName)
                .schema(schema)
                .value(value)
                .build();
        for (Rule rule : rules) {
            CleanPlan cleanPlan =
                new CleanPlan(DBConnectionFactory.getSourceDBConfig(), rule);
            cleanPlans.add(cleanPlan);
            executors.add(new CleanExecutor(cleanPlan));
        }
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
        try {
            cleanPlans = CleanPlan.createCleanPlanFromJSON(new FileReader(file));
        } catch (InvalidCleanPlanException ex) {
            tracer.err("Invalid CleanPlan file", ex);
            return;
        } catch (InvalidRuleException ex) {
            tracer.err("Invalid Rule definition", ex);
            return;
        }

        for (CleanPlan cleanPlan : cleanPlans) {
            executors.add(new CleanExecutor(cleanPlan));
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
                Thread.sleep(500);
                double percentage = executor.getDetectPercentage();
                printProgress(percentage, "DETECT");
            } while (thread.isAlive());

            // print out the final result.
            String ruleName = executor.getCleanPlan().getRule().getRuleName();
            double percentage = executor.getDetectPercentage();
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
                Thread.sleep(500);
                double percentage = executor.getRepairPercentage();
                printProgress(percentage, "REPAIR");
            } while (thread.isAlive());

            // print out the final result.
            String ruleName = executor.getCleanPlan().getRule().getRuleName();
            double percentage = executor.getRepairPercentage();
            printProgress(percentage, "REPAIR");
            console.println();
            console.flush();
            Tracer.printRepairSummary(ruleName);
        }
    }

    private static void run(String cmd) throws IOException, InterruptedException {
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            console.println(
                "Wrong command. Run run [id number] instead."
            );
        }

        int index = -1;
        if (tokens.length == 2) {
            index = Integer.valueOf(tokens[1]);
            if (index < 0 || index >= cleanPlans.size()) {
                console.println("Out of rule index.");
                return;
            }
        }

        for (int i = 0; i < executors.size(); i ++) {
            if (index != -1 && i != index) {
                continue;
            }

            CleanExecutor executor = executors.get(i);
            Thread thread = new Thread(new CleanRunnable(executor));
            thread.start();

            do {
                Thread.sleep(500);
                double percentage = executor.getRunPercentage();
                printProgress(percentage, "CLEAN");
            } while (thread.isAlive());

            // print out the final result.
            String ruleName = executor.getCleanPlan().getRule().getRuleName();
            double percentage = executor.getRunPercentage();
            printProgress(percentage, "CLEAN");
            console.println();
            console.flush();
            Tracer.printDetectSummary(ruleName);
            Tracer.printRepairSummary(ruleName);
        }
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
                " |    run both detect and clean with a given rule id number. \n" +
                " |\n" +
                " |schema [table name]: \n" +
                " |    list the table schema from the data source. \n" +
                " |\n" +
                " |fd [table name] [fd rule value]: \n" +
                " |    create FD rule with specified table name on the source data. \n" +
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
