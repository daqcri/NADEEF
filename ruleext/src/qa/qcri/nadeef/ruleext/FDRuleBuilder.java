/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.ruleext;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * Template engine for FD rule.
 */
public class FDRuleBuilder extends RuleBuilder {
    //<editor-fold desc="Private fields">
    private List<String> lhs;
    private List<String> rhs;
    private static Tracer tracer = Tracer.getTracer(FDRuleBuilder.class);
    private static ST st;
    //</editor-fold>

    //<editor-fold desc="Public methods">
    /**
     * Compiles the given FD rule. If the .class file already exists in the file the
     * compilation is skipped.
     * @return returns Class name.
     */
    @Override
    public Collection<File> compile() throws IOException {
        File inputFile = generate();
        // check whether the class file is already there, if so we skip the compiling phrase.
        String fullPath = inputFile.getAbsolutePath();
        File classFile = new File(fullPath.replace(".java", ".class"));
        if (classFile.exists() || CommonTools.compileFile(inputFile)) {
            return Lists.newArrayList(classFile);
        }

        return null;
    }

    //</editor-fold>
    /**
     * Generates the code.
     * @return generated file in full path.
     */
    protected File generate() throws IOException {
        STGroupFile stFile =
            new STGroupFile("qa/qcri/nadeef/ruleext/template/FDRuleBuilder.stg", '$', '$');
        st = stFile.getInstanceOf("fdTemplate");
        st.add("leftHandSideInitialize", lhs);
        st.add("rightHandSideInitialize", rhs);
        if (Strings.isNullOrEmpty(ruleName)) {
            ruleName = "DefaultFD" + CommonTools.toHashCode(value.get(0));
        } else {
            // remove all the empty spaces to make it a valid class name.
            ruleName = ruleName.replace(" ", "");
        }

        st.add("FDName", ruleName);

        File outputFile = getOutputFile();
        st.write(outputFile, null);
        return outputFile;
    }

    @Override
    protected void parse() {
        Set<String> lhsSet = new HashSet();
        Set<String> rhsSet = new HashSet();

        // Here we assume the rule comes in with one line.
        String line = value.get(0);
        String[] tokens = line.trim().split("\\|");
        String token;
        if (tokens.length != 2) {
            throw new IllegalArgumentException("Invalid rule description " + line);
        }

        // parse the LHS
        String[] lhsSplits = tokens[0].split(",");
        // use the first one as default table name.
        String defaultTable = tableNames.get(0);
        String newColumn = null;
        for (int i = 0; i < lhsSplits.length; i ++) {
            token = lhsSplits[i].trim().toLowerCase();
            if (Strings.isNullOrEmpty(token)) {
                throw new IllegalArgumentException("Invalid rule description " + line);
            }

            if (!CommonTools.isValidColumnName(token)) {
                newColumn = defaultTable + "." + token;
            } else {
                newColumn = token;
            }

            if (lhsSet.contains(newColumn)) {
                throw new IllegalArgumentException(
                    "FD cannot have duplicated column " + newColumn
                );
            }
            lhsSet.add(newColumn);
        }

        // parse the RHS
        String[] rhsSplits = tokens[1].trim().split(",");
        for (int i = 0; i < rhsSplits.length; i ++) {
            token = rhsSplits[i].trim().toLowerCase();
            if (token.isEmpty()) {
                throw new IllegalArgumentException("Invalid rule description " + line);
            }

            if (!CommonTools.isValidColumnName(token)) {
                newColumn = defaultTable + "." + token;
            } else {
                newColumn = token;
            }

            if (rhsSet.contains(newColumn)) {
                throw new IllegalArgumentException(
                    "FD cannot have duplicated column " + newColumn
                );
            }
            rhsSet.add(newColumn);
        }

        lhs = Lists.newArrayList(lhsSet);
        rhs = Lists.newArrayList(rhsSet);
    }
}
