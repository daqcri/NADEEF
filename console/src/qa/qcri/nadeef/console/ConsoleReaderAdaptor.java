/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.console;

import jline.console.ConsoleReader;

import java.io.IOException;
import java.io.PrintStream;

/**
 * ConsoleReader to PrintStream adapter.
 */
public class ConsoleReaderAdaptor extends PrintStream {
    private ConsoleReader consoleReader;
    public ConsoleReaderAdaptor(ConsoleReader consoleReader) {
        super(System.out);
        this.consoleReader = consoleReader;
    }

    @Override
    public void println() {
        try {
            consoleReader.println();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void println(String string) {
        try {
            consoleReader.println(string);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void print(String string) {
        try {
            consoleReader.print(string);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
        try {
            consoleReader.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
