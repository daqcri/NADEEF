/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Strings;

/**
 * Table attribute object, used as the operand in the rule hint.
 */
public class Column {
    private String tableName;
    private String attributeName;
    private String schemaName;

    //<editor-fold desc="Constructors">
    /**
     * Constructor.
     */
    public Column(String tableName, String attributeName) {
        this("public", tableName, attributeName);
    }

    /**
     * Constructor.
     */
    public Column(String fullAttributeName) {
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
    public Column(String schemaName, String tableName, String attributeName) {
        if (attributeName == null || tableName == null || schemaName == null) {
            throw new IllegalArgumentException("Attribute name cannot be null.");
        }
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.attributeName = attributeName;
    }
    //</editor-fold>

    /**
     * Gets the schema name.
     * @return schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Gets the table name. It also deals with situation where the table is a view.
     * @return original table name.
     */
    public String getTableName() {
        if (tableName.startsWith("VIEW")) {
            String[] tokens = tableName.split("_");
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i < tokens.length - 1; i ++) {
                if (i != 1) {
                    builder.append('_');
                }
                builder.append(tokens[i]);
            }
            return builder.toString();
        }
        return tableName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    /**
     * Generates a string with format of 'schemaName'.'tableName'.'attributeName'.
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

        if (obj == null || !(obj instanceof Column)) {
            return false;
        }

        Column column = (Column)obj;
        if (
            column.getAttributeName().equalsIgnoreCase(attributeName) &&
            column.getSchemaName().equalsIgnoreCase(schemaName) &&
            column.getTableName().equalsIgnoreCase(getTableName())
        ) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int root = 109;
        return
            root * getTableName().toLowerCase().hashCode() *
                attributeName.toLowerCase().hashCode() *
                schemaName.toLowerCase().hashCode();
    }
    //</editor-fold>

}