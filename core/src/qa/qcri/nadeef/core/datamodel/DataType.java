package qa.qcri.nadeef.core.datamodel;

import java.sql.Types;

/**
 * Primitive data types used in abstract rule.
 */
public enum DataType {
    STRING(0),
    INTEGER(1),
    DOUBLE(2),
    FLOAT(3);

    private final int value;
    private DataType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    // this is a bug in MySQL JDBC where getType doesn't match the sql.Types
    public static DataType getDataType(String sqlTypeValue) {
        DataType result;
        if (sqlTypeValue.equalsIgnoreCase("serial"))
            result = DataType.INTEGER;
        else if (sqlTypeValue.equalsIgnoreCase("int"))
            result = DataType.INTEGER;
        else if (sqlTypeValue.equalsIgnoreCase("integer"))
            result = DataType.INTEGER;
        else if (sqlTypeValue.equalsIgnoreCase("varchar"))
            result = DataType.STRING;
        else if (sqlTypeValue.equalsIgnoreCase("float"))
            result = DataType.FLOAT;
        else if (sqlTypeValue.equalsIgnoreCase("float8"))
            result = DataType.FLOAT;
        else if (sqlTypeValue.equalsIgnoreCase("double"))
            result = DataType.DOUBLE;
        else
            throw new IllegalArgumentException("Unknown data types " + sqlTypeValue);
        return result;
    }

    public static DataType getDataType(int sqlTypeValue) {
        DataType result;
        switch (sqlTypeValue) {
            case Types.INTEGER:
                result = DataType.INTEGER;
                break;
            case Types.VARCHAR:
                result = DataType.STRING;
                break;
            case Types.FLOAT:
                result = DataType.FLOAT;
                break;
            case Types.DOUBLE:
                result = DataType.DOUBLE;
                break;
            default:
                throw new IllegalArgumentException("Unknown data types " + sqlTypeValue);
        }
        return result;
    }
}
