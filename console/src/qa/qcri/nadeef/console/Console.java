/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.console;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import jline.console.ConsoleReader;
import jline.console.completer.*;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBMetaDataTool;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.FileHelper;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
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
    private static CleanPlan currentCleanPlan;
    private static CleanExecutor executor;

    //</editor-fold>

    /**
     * Start of Console.
     * @param args user input.
     */
    public static void main(String[] args) {
        try {
            // bootstrap Nadeef.
            Stopwatch stopwatch = new Stopwatch().start();
            Bootstrap.Start();
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

        if (currentCleanPlan == null) {
            throw new NullPointerException(
                "There is no CleanPlan loaded."
            );
        }

        String tableName = splits[1];
        if (!currentCleanPlan.getTableNames().contains(tableName)) {
            throw new IllegalArgumentException("Unknown table names.");
        }

        Schema schema = DBMetaDataTool.getSchema(currentCleanPlan.getSourceDBConfig(), tableName);
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
        if (ruleBuilder != null) {
            currentCleanPlan.getRules().addAll(
                ruleBuilder.name("UserRule" + CommonTools.toHashCode(value)).value(value).build()
            );
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
            currentCleanPlan = CleanPlan.createCleanPlanFromJSON(new FileReader(file));
        } catch (Exception ex) {
            console.println("Exception happens during loading JSON file: " + ex.getMessage());
            return;
        }

        executor = CleanExecutor.getInstance();
        executor.initialize(currentCleanPlan);
        console.println(
            currentCleanPlan.getRules().size()
                + " rules loaded in "
                + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms."
        );
    }

    private static void list() throws IOException {
        if (currentCleanPlan == null) {
            console.println("There are 0 rules loaded.");
            return;
        }

        List<Rule> rules = currentCleanPlan.getRules();
        console.println("There are " + rules.size() + " rules loaded.");
        for (int i = 0; i < rules.size(); i ++) {
            console.println("\t" + i + ": " + rules.get(i).getId());
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

        if (executor == null) {
            console.println("There is no rule loaded.");
            return;
        }

        if (tokens.length == 1) {
            executor.detect();
        } else {
            executor.detect(Integer.valueOf(tokens[1]));
        }

        Tracer.printDetectSummary();
    }

    private static void repair(String cmd) {
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            throw
                new IllegalArgumentException(
                    "Wrong detect command. Run detect [id number] instead."
                );
        }

        if (tokens.length == 1) {
            executor.repair();
        } else {
            executor.repair(Integer.valueOf(tokens[1]));
        }

        Tracer.printRepairSummary();
    }

    private static void run(String cmd) {
        String[] tokens = cmd.split("\\s");
        if (tokens.length > 2) {
            throw
                new IllegalArgumentException(
                    "Wrong detect command. Run detect [id number] instead."
                );
        }

        if (tokens.length == 1) {
            executor.run();
        } else {
            executor.run(Integer.valueOf(tokens[1]));
        }

        Tracer.printDetectSummary();
        Tracer.printRepairSummary();
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
