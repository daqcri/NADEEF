package qa.qcri.nadeef.core.datamodel;

import java.sql.Types;

/**
 * Primitive data types used in abstract rule.
 */
public enum DataType {
    STRING(0),
    INTEGER(1),
    DOUBLE(2);

    private final int value;
    private DataType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static DataType getDataType(int sqlTypeValue) {
        DataType result;
        switch (sqlTypeValue) {
            case Types.INTEGER:
                result = DataType.INTEGER;
                break;
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                result = DataType.STRING;
                break;
            case Types.DOUBLE:
                result = DataType.DOUBLE;
                break;
            default:
                throw new IllegalArgumentException("Unknown data types.");
        }
        return result;
    }
}
