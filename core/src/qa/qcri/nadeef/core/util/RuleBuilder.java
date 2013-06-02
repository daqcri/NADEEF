/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class RuleBuilder represents the extension point of writing new
 * abstract rules (e.g. FD).
 */
public abstract class RuleBuilder {
	// <editor-fold desc="Private fields">
	protected List<String> value;
	protected String originalRuleName;
	protected String ruleName;
	protected List<String> tableNames;
	protected List<Schema> schemas;
	protected File outputPath;

	// </editor-fold>

	public RuleBuilder() {}

	// <editor-fold desc="Public methods">
	/**
	 * Sets the rule value.
	 * 
	 * @param value rule value.
	 */
	public RuleBuilder value(String value) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
		this.value = Lists.newArrayList(value);
		parse();
		return this;
	}

	/**
	 * Sets the source of the Rule.
	 * 
	 * @param schemas input schemas.
	 */
	public RuleBuilder schema(List<Schema> schemas) {
		this.schemas = Preconditions.checkNotNull(schemas);
		return this;
	}

	/**
	 * Sets the source of the Rule.
	 * 
	 * @param schema input schemas.
	 */
	public RuleBuilder schema(Schema schema) {
		Preconditions.checkNotNull(schema);
		schemas = Lists.newArrayList(schema);
		return this;
	}

	/**
	 * Sets the rule value.
	 * 
	 * @param values rule value.
	 */
	public RuleBuilder value(List<String> values) {
		this.value = Preconditions.checkNotNull(values);
		parse();
		return this;
	}

	/**
	 * Sets the table names.
	 * 
	 * @param tableNames table name.
	 */
	public RuleBuilder table(List<String> tableNames) {
		Preconditions.checkArgument(tableNames != null && tableNames.size() > 0);
		this.tableNames = tableNames;
		return this;
	}

	/**
	 * Sets the table names.
	 * 
	 * @param table table name.
	 */
	public RuleBuilder table(String table) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(table));
		this.tableNames = Lists.newArrayList(table);
		return this;
	}

	/**
	 * Sets the output path.
	 * 
	 * @param outputPath output path.
	 */
	public RuleBuilder out(File outputPath) {
		Preconditions.checkArgument(outputPath != null && outputPath.isDirectory());
		this.outputPath = outputPath;
		return this;
	}

	/**
	 * Sets the rule name.
	 * 
	 * @param ruleName rule name.
	 */
	public RuleBuilder name(String ruleName) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(ruleName));
		this.originalRuleName = ruleName;
		this.ruleName = ruleName;
		return this;
	}

	/**
	 * Generates the <code>Rule</code> class code.
	 * 
	 * @return generated rule class.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Collection<Rule> build() throws Exception {
		List<Rule> result = Lists.newArrayList();
		Collection<File> outputFiles = compile();
		Tracer tracer = Tracer.getTracer(RuleBuilder.class);
		for (File outputFile : outputFiles) {
			Stopwatch stopwatch = new Stopwatch().start();
			String className =
                Files.getNameWithoutExtension(outputFile.getName());
			URL url =
                new URL("file://" + outputFile.getParent() + File.separator);
			Class ruleClass = CommonTools.loadClass(className, url);
			Rule rule = (Rule) ruleClass.getConstructor().newInstance();

            rule.initialize(
				outputFile.getName().substring( 0,outputFile.getName().lastIndexOf('.')),
                tableNames
            );
			result.add(rule);
			tracer.verbose("Rule file : " + outputFile.getAbsolutePath()
					+ " is loaded in "
					+ stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms.");
			stopwatch.stop();
		}
		return result;
	}

	/**
	 * Generates and compiles the rule .class file without loading it.
	 * 
	 * @return Output class file.
	 */
	public abstract Collection<File> compile() throws Exception;

	// </editor-fold>

	// <editor-fold desc="Protected Fields">
	/**
	 * Gets the compiled output file.
	 * 
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
	protected void parse() {
	}
	// </editor-fold>
}
