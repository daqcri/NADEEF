/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.console;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.SimpleCompletor;

/**
 * User interactive console.
 */
public class Console {

    //<editor-fold desc="Private fields">
    private static final String logo =
            "   _  __        __        _____\n" +
            "  / |/ /__ ____/ /__ ___ /   _/\n" +
            " /    / _ `/ _  / -_) -_)   _/\n" +
            "/_/|_/\\_,_/\\_,_/\\__/\\__/  _/\n" +
            "Data Cleaning solution (Build " + System.getenv("BuildVersion")  +
            ", using Java " + System.getProperty("java.version") + ").\n" +
            "Copyright (C) Qatar Computing Research Institute, 2013";

    private static final String helpInfo = "Type 'help' to see what commands we have.";

    private static final String prompt = "::> ";

    private static final String[] commands = { "load", "run", "repair", "help", "set", "exit" };
    //</editor-fold>

    public static void main(String[] args) {
        try {
            System.out.println(logo);
            System.out.println();
            System.out.println(helpInfo);
            System.out.println();

            String line;
            ConsoleReader console = new ConsoleReader();
            console.setDefaultPrompt(prompt);
            console.addCompletor(new ArgumentCompletor(new SimpleCompletor(commands)));
            console.addCompletor(new ArgumentCompletor(new FileNameCompletor()));
            while ((line = console.readLine()) != null) {
                if (line == "exit") {
                    break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        System.exit(0);
    }
}
