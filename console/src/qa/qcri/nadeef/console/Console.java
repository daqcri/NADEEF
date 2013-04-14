/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.console;

import jline.console.ConsoleReader;
import jline.console.completer.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            "Copyright (C) Qatar Computing Research Institute, 2013.";

    private static final String helpInfo = "Type 'help' to see what commands we have.";

    private static final String prompt = ":> ";

    private static final String[] commands = { "load", "run", "repair", "help", "set", "exit" };
    //</editor-fold>

    /**
     * Start of Console.
     * @param args user input.
     */
    public static void main(String[] args) {
        try {
            ConsoleReader console = new ConsoleReader();
            console.println(logo);
            console.println();
            console.println(helpInfo);
            console.println();

            String line;

            console.setPrompt(prompt);
            List<Completer> completers = new ArrayList<>();
            completers.add(new StringsCompleter(commands));
            completers.add(new FileNameCompleter());
            completers.add(new NullCompleter());
            console.addCompleter(new ArgumentCompleter(completers));

            while ((line = console.readLine()) != null) {
                line = line.trim();
                if (line.equalsIgnoreCase("exit")) {
                    break;
                }

                if (line.equalsIgnoreCase("help")) {
                    printHelp(console);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        System.exit(0);
    }

    public static void printHelp(ConsoleReader console) throws IOException {
        String help =
                " |Nadeef console usage:\n" +
                " |----------------------------------\n" +
                " |help : Print out this help information.\n" +
                " |\n" +
                " |load <input CleanPlan file> :\n" +
                " |    load a configured nadeef file.\n" +
                " |\n" +
                " |detect <rule name> :\n" +
                " |    start the violation detection with a given rule name.\n" +
                " |\n" +
                " |list : \n" +
                " |    list the available rule.\n" +
                " |\n" +
                " |repair <target table name> :\n" +
                " |    repair the data source.\n" +
                " |\n" +
                " |exit :\n" +
                " |    exit the console (Ctrl + D).\n";
        console.println(help);
    }
}
