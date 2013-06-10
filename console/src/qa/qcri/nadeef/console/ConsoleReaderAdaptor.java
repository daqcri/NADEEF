/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
