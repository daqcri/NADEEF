package qa.qcri.nadeef.core.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLDialect implements IDialect {

	private static final String INDEX_CREATION_TEMPLATE = "CREATE INDEX IDX_{TABLENAME}_{COLUMNNAME} ON {TABLENAME} ({COLUMNNAME})";
	
	private static final String INDEX_DROP_TEMPLATE = "DROP INDEX IDX_{TABLENAME}_{COLUMNNAME}";
	
	private static final String TABLE_DROP_TEMPLATE = "DROP TABLE IF EXISTS {TABLENAME} CASCADE";
	
	private static final String SELECT_SCHEMA_FOR_TABLE = "SELECT * FROM {TABLENAME} WHERE 1=0";
	
	private static final String ADD_COLUMN_AS_SERIAL_PRIMARY_KEY = "ALTER TABLE {TABLENAME} ADD COLUMN {COLUMNNAME} SERIAL PRIMARY KEY";
	
	@Override
	public void copy(Connection conn, String sourceTableName,
			String targetTableName) throws SQLException{
		Statement stat = conn.createStatement();
	 	stat.execute("CREATE TABLE " + targetTableName + " LIKE  " + sourceTableName);
    	stat.execute("INSERT INTO " + targetTableName + " SELECT * FROM  " + sourceTableName);
    	conn.commit();
	}

	@Override
	public void createIndex(Connection conn, String tableName, String columnName) throws SQLException {
		Statement stat = conn.createStatement();
		String createIndex = INDEX_CREATION_TEMPLATE.replaceAll("\\{TABLENAME\\}", tableName).replaceAll("\\{COLUMNNAME\\}", columnName);
		stat.execute(createIndex);
		conn.commit();
	}

	@Override
	public void dropIndex(Connection conn, String tableName, String columnName) throws SQLException {
		Statement stat = conn.createStatement();
		String dropIndex = INDEX_DROP_TEMPLATE.replaceAll("\\{TABLENAME\\}", tableName).replaceAll("\\{COLUMNNAME\\}", columnName);
		stat.execute(dropIndex);
		conn.commit();
	}

	@Override
	public void dropTable(Connection conn, String tableName) throws SQLException{
		Statement stat = conn.createStatement();
		String dropIndex = TABLE_DROP_TEMPLATE.replaceAll("\\{TABLENAME\\}", tableName);
		stat.execute(dropIndex);
		conn.commit();
	}

	@Override
	public void addColumnAsSerialPrimaryKeyIfNotExists(Connection conn, String tableName, String columnName)
			throws SQLException {
		Statement stat = conn.createStatement();
		String selectSchema = SELECT_SCHEMA_FOR_TABLE.replaceAll("\\{TABLENAME\\}", tableName);
		ResultSet set = stat.executeQuery(selectSchema);
		ResultSetMetaData rsmd = set.getMetaData();
	    int columns = rsmd.getColumnCount();
	    for (int x = 1; x <= columns; x++) {
	        if (columnName.equalsIgnoreCase(rsmd.getColumnName(x))) {
	        	// TODO propogate exception here
	            return;
	        }
	    }
	    String addColumn = ADD_COLUMN_AS_SERIAL_PRIMARY_KEY.replaceAll("\\{TABLENAME\\}", tableName).replaceAll("\\{COLUMNNAME\\}", columnName);
	    stat.execute(addColumn);
	    conn.commit();
	}
}
