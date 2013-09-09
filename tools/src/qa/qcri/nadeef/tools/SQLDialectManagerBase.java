/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.tools;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Base class for cross vendor Database calling.
 */
public abstract class SQLDialectManagerBase {

    /**
     * Builds up JDBC connection url.
     * @param url url.
     * @param dialect sql dialect.
     * @return JDBC connection url string.
     */
    public static String buildJdbcUrl(String url, SQLDialect dialect) {
        StringBuilder jdbcUrl = new StringBuilder("jdbc:");
        switch (dialect) {
            default:
            case DERBY:
                jdbcUrl.append("derby:").append(url);
                break;
            case POSTGRES:
                jdbcUrl.append("postgresql://").append(url);
                break;
            case MYSQL:
                jdbcUrl.append("mysql://").append(url);
                break;
        }

        return jdbcUrl.toString();
    }

    /**
     * Gets the {@link qa.qcri.nadeef.tools.SQLDialect} from a string.
     * @param type type string.
     * @return sql dialect.
     */
    public static SQLDialect getSQLDialect(String type) {
        SQLDialect result;
        switch (type) {
            default:
            case "derby":
                result = SQLDialect.DERBY;
                break;
            case "postgres":
                result = SQLDialect.POSTGRES;
                break;
            case "mysql":
                result = SQLDialect.MYSQL;
                break;
        }
        return result;
    }

    /**
     * Copy table.
     * @param sourceName source name.
     * @param targetName target name.
     */
    public abstract void copyTable(
        Statement stat,
        String sourceName,
        String targetName
    ) throws SQLException;

    /**
     * Drop table.
     * @param tableName drop table name.
     * @return SQL statement.
     */
    public String dropTable(String tableName) {
        return "DROP TABLE " + tableName;
    }

    /**
     * Select star..
     * @param tableName table name.
     * @return SQL statement.
     */
    public String selectAll(String tableName) {
        return "SELECT * FROM " + tableName;
    }

    /**
     * Count the number of rows in the table.
     * @param tableName table name.
     * @return SQL statement.
     */
    public abstract String countTable(String tableName);

    /**
     * Limits the select.
     * @param row row number.
     * @return SQL statement.
     */
    public abstract String limitRow(int row);

    /**
     * Inserts values into a table from CSV row used for batch loading.
     * @param metaData column meta data.
     * @param tableName target table name.
     * @param row row value.
     * @return SQL statement.
     */
    public abstract String importFromCSV(
        ResultSetMetaData metaData,
        String tableName,
        String row
    );

    /**
     * Creates a table in the database from a CSV file header.
     * @param tableName table name.
     * @param content table description.
     * @return SQL statement.
     */
    public abstract String createTableFromCSV(String tableName, String content);

}
