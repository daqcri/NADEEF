/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.console;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import jline.console.ConsoleReader;
import jline.console.completer.*;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.DBMetaDataTool;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.FileHelper;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

    //</editor-fold>

    /**
     * Start of Console.
     * @param args user input.
     */
    public static void main(String[] args) {
        try {
            // bootstrap Nadeef.
            Stopwatch stopwatch = new Stopwatch().start();
            Bootstrap.start();
            String line;

            console = new ConsoleReader();
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

            while ((line = console.readLine()) != null) {
                line = line.trim();
                Tracer.recreateStat();
                try {
                    if (line.startsWith("exit")) {
                        break;
                    } else if (line.startsWith("load")) {
                        load(line);
                    } else if (line.startsWith("list")) {
                        list();
                    } else if (line.startsWith("help")) {
                        printHelp();
                    } else if (line.startsWith("detect")) {
                        detect(line);
                    } else if (line.startsWith("repair")) {
                        repair(line);
                    } else if (line.startsWith("run")) {
                        run(line);
                    } else if (Strings.isNullOrEmpty(line)) {
                        // empty line
                    } else if (line.startsWith("set")) {
                        set(line);
                    } else if (line.startsWith("fd")) {
                        fd(line);
                    } else if (line.startsWith("schema")) {
                        schema(line);
                    } else if (line.startsWith("test")) {
                        testPrint(line);
                    } else {
                        console.println("I don't know this command.");
                    }
                } catch (Exception ex) {
                    console.println("Oops, something is wrong:");
                    ex.printStackTrace();
                }
            }
        } catch (Exception ex) {
            try {
                console.println("Bootstrap failed.");
                ex.printStackTrace();
            } catch (Exception ignore) {}
        } finally {
            Bootstrap.shutdown();
        }

        System.exit(0);
    }

    private static void schema(String cmdLine) throws Exception {
        String[] splits = cmdLine.split("\\s");
        if (splits.length != 2) {
            throw new IllegalArgumentException(
                "Invalid schema command. Please try to use schema [table name]."
            );
        }

        if (cleanPlans == null) {
            throw new NullPointerException(
                "There is no CleanPlan loaded."
            );
        }

        String tableName = splits[1];
        if (!DBMetaDataTool.isTableExist(tableName)) {
            throw new IllegalArgumentException("Unknown table names.");
        }

        Schema schema = DBMetaDataTool.getSchema(tableName);
        Set<Column> columns = schema.getColumns();
        for (Column column : columns) {
            if (column.getAttributeName().equals("tid")) {
                continue;
            }
            console.println(String.format("\t%s", column.getAttributeName()));
        }
    }

    // TODO: remove FD specification, and make it generic
    private static void fd(String cmdLine) throws Exception {
        int index = cmdLine.indexOf("fd") + 2;
        String value = cmdLine.substring(index);
        RuleBuilder ruleBuilder = NadeefConfiguration.tryGetRuleBuilder("fd");
        Collection<Rule> rules =
            ruleBuilder
                .name("UserRule" + CommonTools.toHashCode(value))
                .value(value)
                .build();
        for (Rule rule : rules) {
            CleanPlan cleanPlan =
                new CleanPlan(DBConnectionFactory.getSourceDBConfig(), rule);
            cleanPlans.add(cleanPlan);
        }
    }

    private static void load(String cmdLine) throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        String[] splits = cmdLine.split("\\s");
        if (splits.length != 2) {
            throw new IllegalArgumentException(
                "Invalid load command. Run load <Nadeef config file>."
            );
        }
        String fileName = splits[1];
        File file = FileHelper.getFile(fileName);
        try {
            cleanPlans = CleanPlan.createCleanPlanFromJSON(new FileReader(file));
        } catch (Exception ex) {
            console.println("Exception happens during loading JSON file: " + ex.getMessage());
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
            console.println("There are 0 rules loaded.");
            return;
        }

        console.println("There are " + cleanPlans.size() + " rules loaded.");
        for (int i = 0; i < cleanPlans.size(); i ++) {
            console.println("\t" + i + ": " + cleanPlans.get(i).getRule().getId());
        }
    }

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

    private static void detect(String cmd) throws IOException {
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            throw
                new IllegalArgumentException(
                    "Wrong detect command. Run detect [id number] instead."
                );
        }

        if (executors == null) {
            console.println("There is no rule loaded.");
            return;
        }

        CleanExecutor executor;
        if (tokens.length == 1) {
            executor = executors.get(0);
        } else {
            int index = Integer.valueOf(tokens[1]);
            if (index >= 0 && index < cleanPlans.size()) {
                executor = executors.get(index);
            } else {
                console.println("Out of index.");
                return;
            }
        }

        Thread thread = new Thread(new DetectRunnable(executor));
        thread.start();

        while (thread.isAlive()) {
            double percentage = executor.getDetectPercentage();
            printProgress(percentage, "Detect");
        }
    }

    private static void testPrint(String cmd) throws Exception {
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            throw
                new IllegalArgumentException(
                    "Wrong detect command. Run detect [id number] instead."
                );
        }

        for (double i = 0; i <= 1.0; i += 0.1) {
            printProgress(i, "test");
            Thread.sleep(1000);
        }
        console.println();
    }

    private static void printProgress(double percentage, String title) throws IOException {
        console.redrawLine();
        int ne = (int)Math.round(percentage * 50.f);
        StringBuilder stringBuilder = new StringBuilder("[");
        stringBuilder.append(title).append("][");
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

    private static void repair(String cmd) throws IOException{
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            throw
                new IllegalArgumentException(
                    "Wrong detect command. Run detect [id number] instead."
                );
        }

        if (tokens.length == 1) {
            executors.get(0).repair();
        } else {
            int index = Integer.valueOf(tokens[1]);
            if (index >= 0 && index < cleanPlans.size()) {
                executors.get(index).repair();
            } else {
                console.println("Out of index.");
            }
        }
    }

    private static void run(String cmd) throws IOException {
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            throw
                new IllegalArgumentException(
                    "Wrong detect command. Run detect [id number] instead."
                );
        }

        if (tokens.length == 1) {
            executors.get(0).run();
        } else {
            int index = Integer.valueOf(tokens[1]);
            if (index >= 0 && index < cleanPlans.size()) {
                executors.get(index).run();
            } else {
                console.println("Out of index.");
            }
        }
    }

    private static void set(String cmd) throws IOException {
        String[] splits = cmd.split("\\s");
        if (splits[1].equalsIgnoreCase("verbose")) {
            boolean mode = !Tracer.isVerboseOn();
            console.println("set verbose " + (mode ? "on" : "off"));
            Tracer.setVerbose(mode);
        }

        if (splits[1].equalsIgnoreCase("info")) {
            boolean mode = !Tracer.isInfoOn();
            console.println("set info " + (mode ? "on" : "off"));
            Tracer.setInfo(mode);
        }
    }

    private static void printHelp() throws IOException {
        String help =
                " |Nadeef console usage:\n" +
                " |----------------------------------\n" +
                " |help : Print out this help information.\n" +
                " |\n" +
                " |load <input CleanPlan file> :\n" +
                " |    load a configured nadeef clean plan.\n" +
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
                " |run :\n" +
                " |    clean the data source with maximum iteration number. \n" +
                " |\n" +
                " |schema [table name]: \n" +
                " |    list the schema of the data source. \n" +
                " |fd [table name] [fd rule]: \n" +
                " |    run FD rule with specified table name on the source data. \n" +
                " |exit :\n" +
                " |    exit the console (Ctrl + D).\n";
        console.println(help);
    }
}
