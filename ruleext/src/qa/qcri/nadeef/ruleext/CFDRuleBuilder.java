/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.ruleext;

import java.io.File;
import java.util.Collection;
import java.util.List;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.SimpleExpression;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.tools.CommonTools;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Template engine for CFD rule.
 */
public class CFDRuleBuilder extends RuleBuilder {
    protected List<String> lhs;
    protected List<String> rhs;
    protected List<List<SimpleExpression>> filterExpressions;
    protected STGroupFile stGroup;

    /**
     * Generates and compiles the rule .class file without loading it.
     *
     * @return Output class file.
     */
    @Override
    public Collection<File> compile() throws Exception {
        List<File> result = Lists.newArrayList();
        stGroup =
            new STGroupFile("qa/qcri/nadeef/ruleext/template/PCFDRuleBuilder.stg", '$', '$');
        ST st = stGroup.getInstanceOf("cfdTemplate");
        st.add("leftHandSide", lhs);
        st.add("rightHandSide", rhs);

        for (int i = 0; i < filterExpressions.size(); i ++) {
            if (Strings.isNullOrEmpty(ruleName)) {
                ruleName = "DefaultCFD" + CommonTools.toHashCode(value.get(i + 1)) + "_" + i;
            } else {
                // remove all the empty spaces to make it a valid class name.
                ruleName = originalRuleName.replace(" ", "") + "_" + i;
            }

            st.add("CFDName", ruleName);
            List<SimpleExpression> filters = filterExpressions.get(i);
            ST expressionST = stGroup.getInstanceOf("expressionItem");
            List<String> expressions = Lists.newArrayList();
            for (int j = 0; j < filters.size(); j ++) {
                SimpleExpression expression = filters.get(j);
                expressionST.add("columnName", expression.getLeft().getFullAttributeName());
                expressionST.add("value", expression.getValue());
                expressions.add(expressionST.render());
                expressionST.remove("columnName");
                expressionST.remove("value");
            }
            st.add("expression", expressions);
            File outputFile = getOutputFile();
            st.write(outputFile, null);

            String fullPath = outputFile.getAbsolutePath();
            // skip compiling if the .class file already exists.
            File classFile = new File(fullPath.replace(".java", ".class"));
            if (classFile.exists() || CommonTools.compileFile(outputFile)) {
                result.add(classFile);
            }
            st.remove("expression");
            st.remove("CFDName");
        }

        return result;
    }

    /**
     * Interpret a rule from input text stream.
     */
    @Override
    protected void parse() {
        lhs = Lists.newArrayList();
        rhs = Lists.newArrayList();
        filterExpressions = Lists.newArrayList();

        String head = value.get(0);
        String[] splits = head.trim().split("\\|");
        String split;
        if (splits.length != 2) {
            throw new IllegalArgumentException("Invalid rule description " + head);
        }

        // parse the LHS
        String[] lhsSplits = splits[0].split(",");
        // use the first one as default table name.
        String defaultTable = tableNames.get(0);
        for (int i = 0; i < lhsSplits.length; i ++) {
            split = lhsSplits[i].trim().toLowerCase();
            if (Strings.isNullOrEmpty(split)) {
                throw new IllegalArgumentException("Invalid rule description " + head);
            }

            if (
                !Strings.isNullOrEmpty(defaultTable) &&
                !CommonTools.isValidColumnName(split)
            ) {
                lhs.add(new Column(defaultTable, split).getFullAttributeName());
            } else {
                lhs.add(split);
            }
        }

        // parse the RHS
        String[] rhsSplits = splits[1].trim().split(",");
        for (int i = 0; i < rhsSplits.length; i ++) {
            split = rhsSplits[i].trim().toLowerCase();
            if (split.isEmpty()) {
                throw new IllegalArgumentException("Invalid rule description " + head);
            }

            if (
                !Strings.isNullOrEmpty(defaultTable) &&
                !CommonTools.isValidColumnName(split)
            ) {
                rhs.add(new Column(defaultTable, split).getFullAttributeName());
            } else {
                rhs.add(split);
            }
        }

        // parse condition line
        // TODO: currently we recreate a new FDRule per condition line, but
        // it would have more optimizations based on a buck of lines.
        for (int i = 1; i < value.size(); i ++) {
            List<SimpleExpression> filter = Lists.newArrayList();
            String line = value.get(i);
            if (Strings.isNullOrEmpty(line)) {
                continue;
            }

            splits = line.trim().split(",");
            if (splits.length != lhs.size() + rhs.size()) {
                throw new IllegalArgumentException("Invalid rule description " + line);
            }

            String curColumn;
            for (int j = 0; j < splits.length; j ++) {
                split = splits[j].trim().toLowerCase();
                if (split.equals("_")) {
                    continue;
                }
                if (j < lhs.size()) {
                    curColumn = lhs.get(j);
                } else {
                    curColumn = rhs.get(j - lhs.size());
                }

                if (Strings.isNullOrEmpty(split)) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }

                SimpleExpression newFilter =
                    SimpleExpression.newEqual(new Column(curColumn), split);
                filter.add(newFilter);
            }
            filterExpressions.add(filter);
        }
    }

    /**
     * Gets of LHS.
     * @return LHS.
     */
    public List<String> getLhs() {
        return lhs;
    }

    /**
     * Gets of RHS.
     * @return
     */
    public List<String> getRhs() {
        return rhs;
    }

    /**
     * Gets of exists filter expressions.
     * @return filter expressions.
     */
    public List<List<SimpleExpression>> getFilterExpressions() {
        return filterExpressions;
    }
}
