package qa.qcri.nadeef.core.util;

import java.sql.Connection;
import java.sql.SQLException;

public interface IDialect {
	public void copy(Connection conn,String sourceTableName, String targetTableName) throws SQLException;
	public void createIndex(Connection conn, String tableName, String columnName) throws SQLException;
	public void dropIndex(Connection conn, String tableName, String columnName) throws SQLException;
	public void dropTable(Connection conn, String tableName) throws SQLException;
	public void addColumnAsSerialPrimaryKeyIfNotExists(Connection conn, String tableName, String columnName) throws SQLException;
}
