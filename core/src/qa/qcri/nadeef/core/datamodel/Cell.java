/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Table attribute object, used as the operand in the rule hint.
 */
public class Cell {
    private String tableName;
    private String attributeName;
    private String schemaName;

    /**
     * Constructor.
     */
    public Cell(String tableName, String attributeName) {
        this("public", tableName, attributeName);
    }

    /**
     * Constructor.
     */
    public Cell(String fullAttributeName) {
        if (!isValidFullAttributeName(fullAttributeName)) {
            throw new IllegalArgumentException("Invalid full attribute name " + fullAttributeName);
        }

        String[] splits = fullAttributeName.split("\\.");

        schemaName = "public";
        tableName = splits[0];
        attributeName = splits[1];
    }

    /**
     * Constructor.
     * @param schemaName schema.
     * @param tableName table.
     * @param attributeName attribute.
     */
    public Cell(String schemaName, String tableName, String attributeName) {
        if (attributeName == null || tableName == null || schemaName == null) {
            throw new IllegalArgumentException("Attribute name cannot be null.");
        }
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.attributeName = attributeName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    /**
     * Generates a string with format of 'schemaName'.'tableName'.'atrributeName'.
     */
    public String getFullAttributeName() {
        return tableName + "." + attributeName;
    }

    /**
     * Returns <code>True</code> when the given attribute name is a valid full attribute name.
     * @param attributeName Attribute name.
     * @return <code>True</code> when the attribute name is valid.
     */
    public static boolean isValidFullAttributeName(String attributeName) {
        String[] splits = attributeName.split("\\.");
        // TODO: change to correct regexp.
        if (
            splits.length != 2 ||
            Strings.isNullOrEmpty(splits[0]) || splits[0].contains(" ") ||
            Strings.isNullOrEmpty(splits[1]) || splits[0].contains(" ")
        ) {
            return false;
        }
        return true;
    }

    //<editor-fold desc="Custom equal / hashcode">
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || !(obj instanceof Cell)) {
            return false;
        }

        Cell cell = (Cell)obj;
        if (
            cell.getAttributeName().equals(attributeName) &&
            cell.getSchemaName().equals(schemaName) &&
            cell.getTableName().equals(this.tableName)
        ) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int root = 109;
        return root * tableName.hashCode() * attributeName.hashCode() * schemaName.hashCode();
    }
    //</editor-fold>

}