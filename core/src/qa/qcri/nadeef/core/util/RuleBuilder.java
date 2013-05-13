/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.Rule;

import java.io.File;
import java.util.Collection;
import java.util.List;

/**
 * Abstract class RuleBuilder represents the extension point of writing new
 * abstract rules (e.g. FD).
 */
public abstract class RuleBuilder {
    //<editor-fold desc="Private fields">
    protected List<String> value;
    protected String ruleName;
    protected List<String> tableNames;
    protected File outputPath;
    //</editor-fold>

    public RuleBuilder() {}

    //<editor-fold desc="Public methods">
    /**
     * Sets the rule value.
     * @param value rule value.
     */
    public RuleBuilder value(String value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        this.value = Lists.newArrayList(value);
        parse();
        return this;
    }

    /**
     * Sets the rule value.
     * @param values rule value.
     */
    public RuleBuilder value(List<String> values) {
        this.value = Preconditions.checkNotNull(values);
        parse();
        return this;
    }


    /**
     * Sets the table names.
     * @param tableNames table name.
     */
    public RuleBuilder table(List<String> tableNames) {
        Preconditions.checkArgument(tableNames != null && tableNames.size() > 0);
        this.tableNames = tableNames;
        return this;
    }

    /**
     * Sets the table names.
     * @param table1 table name.
     */
    public RuleBuilder table(String table1) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(table1));
        this.tableNames = Lists.newArrayList(table1);
        return this;
    }

    /**
     * Sets the output path.
     * @param outputPath output path.
     */
    public RuleBuilder out(File outputPath) {
        Preconditions.checkArgument(outputPath != null && outputPath.isDirectory());
        this.outputPath = outputPath;
        return this;
    }

    /**
     * Sets the rule name.
     * @param ruleName rule name.
     */
    public RuleBuilder name(String ruleName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(ruleName));
        this.ruleName = ruleName;
        return this;
    }

    /**
     * Generates the <code>Rule</code> class code.
     * @return generated rule class.
     */
    public abstract Collection<Rule> build() throws Exception;

    /**
     * Generates and compiles the rule .class file without loading it.
     * @return Output class file.
     */
    public abstract File compile() throws Exception;

    //</editor-fold>

    //<editor-fold desc="Protected Fields">
    /**
     * Gets the compiled output file.
     * @return output file.
     */
    protected File getOutputFile() {
        String currentPath;
        if (outputPath == null) {
            currentPath = System.getProperty("user.dir");
        } else {
            currentPath = outputPath.getAbsolutePath();
        }
        return new File(currentPath + File.separator + ruleName + ".java");
    }

    /**
     * Parse the value into a rule.
     */
    protected void parse() {}
    //</editor-fold>
}
