/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * Table attribute object, used as the operand in the rule hint.
 */
public class TableAttribute {
    private String tableName;
    private String attributeName;
    private String schemaName;

    /**
     * Constructor.
     * @param schemaName schema.
     * @param tableName table.
     * @param attributeName attribute.
     */
    public TableAttribute(String schemaName, String tableName, String attributeName) {
        if (attributeName == null) {
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
    public String toFullString() {
        StringBuilder result = new StringBuilder();
        boolean hasPrefix = false;
        if (schemaName != null && !schemaName.isEmpty()) {
            result.append(schemaName);
            hasPrefix = true;
        }

        if (tableName != null && !tableName.isEmpty()) {
            if (hasPrefix) {
                result.append('.');
            }
            result.append(tableName);
            hasPrefix = true;
        }

        if (hasPrefix) {
            result.append('.');
        }
        result.append(attributeName);
        return result.toString();
    }
}