/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.rulewriter;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.stringtemplate.v4.ST;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.util.RuleWriter;
import qa.qcri.nadeef.tools.CommonTools;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Template engine for FD rule.
 */
public class FDRuleBuilder extends RuleWriter {
    //<editor-fold desc="Private fields">
    private List<String> lhs;
    private List<String> rhs;

    private static ST st;

    private static final String template =
        "import qa.qcri.nadeef.core.datamodel.*;\n" +
        "import java.util.*;\n" +

        "public class $FDName$ extends PairTupleRule {\n" +
        "    protected List<Column> leftHandSide;\n" +
        "    protected List<Column> rightHandSide;\n" +
        "    public $FDName$(String id, List<String> tableNames) {\n" +
        "        super(id, tableNames);\n" +
        "        $leftHandSideInitialize$\n" +
        "        $rightHandSideInitialize$\n" +
        "    }\n" +

        "    /**\n" +
        "     * FD scope.\n" +
        "     * @param tupleCollections input tuple collections.\n" +
        "     * @return tuples after filtering.\n" +
        "     */\n" +
        "    @Override\n" +
        "    public Collection<TupleCollection> scope(Collection<TupleCollection> tupleCollections) {\n" +
        "        TupleCollection tupleCollection = tupleCollections.iterator().next();\n" +
        "        tupleCollection.project(leftHandSide).project(rightHandSide);\n" +
        "        return tupleCollections;\n" +
        "    }\n" +

        "    /**\n" +
        "     * Default group operation.\n" +
        "     *\n" +
        "     * @param tupleCollections input tuple\n" +
        "     * @return a group of tuple collection.\n" +
        "     */\n" +
        "    @Override\n" +
        "    public Collection<TuplePair> group(Collection<TupleCollection> tupleCollections) {\n" +
        "        ArrayList<TuplePair> result = new ArrayList();\n" +
        "        TupleCollection tupleCollection = tupleCollections.iterator().next();\n" +
        "        Collection<TupleCollection> groupResult = tupleCollection.groupOn(leftHandSide);\n" +
        "        for (TupleCollection tuples : groupResult) {\n" +
        "            tuples.orderBy(rightHandSide);\n" +
        "            for (int i = 0; i < tuples.size(); i ++) {\n" +
        "                for (int j = i + 1; j < tuples.size(); j ++) {\n" +
        "                    Tuple left = tuples.get(i);\n" +
        "                    Tuple right = tuples.get(j); \n" +
        "                    for (Column column : rightHandSide) {\n" +
        "                        if (!left.get(column).equals(right.get(column))) {\n" +
        "                            TuplePair pair = new TuplePair(tuples.get(i), tuples.get(j));\n"+
        "                            result.add(pair);\n" +
        "                        }\n" +
        "                    }\n" +
        "               }\n" +
        "            }\n" +
        "        }\n" +
        "        return result;\n" +
        "    }\n" +

        "    /**\n" +
        "     * Detect method.\n" +
        "     * @param tuplePair tuple pair.\n" +
        "     * @return violation set.\n" +
        "     */\n" +
        "    @Override\n" +
        "    public Collection<Violation> detect(TuplePair tuplePair) {\n" +
        "        Tuple left = tuplePair.getLeft();\n" +
        "        Tuple right = tuplePair.getRight();\n" +

        "        List<Violation> result = new ArrayList();\n" +
        "        for (Column column : rightHandSide) {\n" +
        "            Object lvalue = left.get(column);\n" +
        "            Object rvalue = right.get(column);\n" +

        "            if (!lvalue.equals(rvalue)) {\n" +
        "                Violation violation = new Violation(id);\n" +
        "                violation.addTuple(left);\n" +
        "                violation.addTuple(right);\n" +
        "                result.add(violation);\n" +
        "                break;\n" +
        "            }\n" +
        "        }\n" +

        "        return result;\n" +
        "    }\n" +

        "    /**\n" +
        "     * Repair of this rule.\n" +
        "     *\n" +
        "     * @param violation violation input.\n" +
        "     * @return a candidate fix.\n" +
        "     */\n" +
        "    @Override\n" +
        "    public Collection<Fix> repair(Violation violation) {\n" +
        "        List<Fix> result = new ArrayList();\n" +
        "        List<Cell> cells = new ArrayList();\n" +
        "        HashMap<Column, Cell> candidates = new HashMap<Column, Cell>();\n" +
        "        int vid = violation.getVid();\n" +
        "        Fix fix;\n" +
        "        Fix.Builder builder = new Fix.Builder(violation);\n" +
        "        for (Cell cell : cells) {\n" +
        "            Column column = cell.getColumn();\n" +
        "            if (rightHandSide.contains(column)) {\n" +
        "                if (candidates.containsKey(column)) {\n" +
        "                    // if the right hand is already found out in another tuple\n" +
        "                    Cell right = candidates.get(column);\n" +
        "                    fix = builder.left(cell).right(right).build();\n" +
        "                    result.add(fix);\n" +
        "                } else {\n" +
        "                    // it is the first time of this cell shown up, put it in the\n" +
        "                    // candidate and wait for the next one shown up.\n" +
        "                    candidates.put(column, cell);\n" +
        "                }\n" +
        "            }\n" +
        "        }\n" +
        "        return result;\n" +
        "    }\n" +
        "}\n";
    //</editor-fold>

    //<editor-fold desc="Public methods">
    /**
     * Generates the <code>Rule</code> class code.
     *
     * @return generated rule class.
     */
    @Override
    public Collection<Rule> build() throws Exception {
        List<Rule> result = Lists.newArrayList();
        File outputFile = compile();
        String className = Files.getNameWithoutExtension(outputFile.getName());
        URL url = new URL("file://" + outputFile.getParent() + File.separator);
        Class ruleClass = CommonTools.loadClass(className, url);
        Rule rule = (Rule)ruleClass.newInstance();
        rule.initialize(ruleName, tableNames);
        result.add(rule);
        return result;
    }

    /**
     * Compiles the given FD rule. If the .class file already exists in the file the
     * compilation is skipped.
     * @return returns Class name.
     */
    @Override
    public File compile() throws IOException {
        File inputFile = generate();
        // check whether the class file is already there, if so we skip the compiling phrase.
        String fullPath = inputFile.getAbsolutePath();
        File classFile = new File(fullPath.replace(".java", ".class"));
        if (classFile.exists() || CommonTools.compileFile(inputFile)) {
            return classFile;
        }

        return null;
    }

    //</editor-fold>
    /**
     * Generates the code.
     * @return generated file in full path.
     */
    protected File generate() throws IOException {
        st = new ST(template, '$', '$');
        // TODO: switch to multiple template group
        ST columnTemplate = new ST("leftHandSide.add(new Column(\"$columnName$\"));\n", '$', '$');
        List<String> leftColumnNames = Lists.newArrayList();
        for (String column : lhs) {
            columnTemplate.add("columnName", column);
            leftColumnNames.add(columnTemplate.render());
            columnTemplate.remove("columnName");
        }
        st.add("leftHandSideInitialize", leftColumnNames);

        columnTemplate = new ST("rightHandSide.add(new Column(\"$columnName$\"));\n", '$', '$');
        List<String> rightColumnNames = Lists.newArrayList();
        for (String column : rhs) {
            columnTemplate.add("columnName", column);
            rightColumnNames.add(columnTemplate.render());
            columnTemplate.remove("columnName");
        }

        st.add("rightHandSideInitialize", rightColumnNames);
        if (Strings.isNullOrEmpty(ruleName)) {
            HashFunction hf = Hashing.md5();
            int hashCode =
                Math.abs(
                    hf.newHasher().putString(value.get(0)).hash().asInt()
                );
            ruleName = "DefaultFD" + hashCode;
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
