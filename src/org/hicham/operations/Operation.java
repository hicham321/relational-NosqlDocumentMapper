package org.hicham.operations;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Operation {
	
	public Connection connectToDB() {
		Connection conn = null;
	    String url = "jdbc:mysql://localhost:3306/";
	    String dbName = "bank";
	    String driver = "com.mysql.jdbc.Driver";
	    String userName = "root";
	    String password = "password";

	    try
	    {
	        Class.forName(driver).newInstance();
	        conn = DriverManager.getConnection(url+dbName,userName,password);
	        //conn.close();
	    }
	    catch (Exception e)
	    {
	        System.out.println("NO CONNECTION =(");
	    }
        return conn;

	}
	//get tables and primary keys of each table
	public void operationsManagement(Connection conn) throws SQLException {
		DatabaseMetaData meta=conn.getMetaData();

		try (ResultSet tables = meta.getTables(null, null, "%", new String[] { "TABLE" })) {
		    while (tables.next()) {
		        String catalog = tables.getString("TABLE_CAT");
		        String schema = tables.getString("TABLE_SCHEM");
		        String tableName = tables.getString("TABLE_NAME");
		        System.out.println("Table: " + tableName);
		        try (ResultSet primaryKeys = meta.getPrimaryKeys(catalog, schema, tableName)) {
		            while (primaryKeys.next()) {
		                System.out.println("Primary key: " + primaryKeys.getString("COLUMN_NAME"));
		            }
		        }
		        // similar for exportedKeys
		    }
		}
	}
	//get table name
	public List<String> tableNames(Connection conn) throws SQLException{
		ArrayList<String> listofTable = new ArrayList<String>();

	    DatabaseMetaData md = conn.getMetaData();

	    ResultSet rs = md.getTables(null, null, "%", null);

	    while (rs.next()) {
	        if (rs.getString(4).equalsIgnoreCase("TABLE")) {
	            listofTable.add(rs.getString(3));
	        }
	    }
	    return listofTable;
	}
	//get all data from a single table
	public List<Map<String, Object>> getTableData(Connection conn, String tableName) throws SQLException {
	     Statement stmt = conn.createStatement();
	     String sql = "SELECT * FROM "+tableName;
	     ResultSet rs = stmt.executeQuery(sql);
	     
	     List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
	     ResultSetMetaData metaData = rs.getMetaData();
	     int columnCount = metaData.getColumnCount();

	     while (rs.next()) {
	         Map<String, Object> columns = new LinkedHashMap<String, Object>();

	         for (int i = 1; i <= columnCount; i++) {
	             columns.put(metaData.getColumnLabel(i), rs.getObject(i));
	         }

	         rows.add(columns);
	     }
	     
	     return rows;
		
	}
	
	//get all the foreign keys of a particular table
	public List<List<String>> getExportedKeys(Connection conn,String tableName) throws SQLException{
		
	    DatabaseMetaData meta = conn.getMetaData();
	    ResultSet rs = meta.getExportedKeys(null, null, tableName);
	    List<List<String>> listOfInfo= new ArrayList<>();
	    List<String> tablesOfFK= new ArrayList<>();
	    List<String> columnNamesOfFK= new ArrayList<>();
	    while (rs.next()) {
	        String fkTableName = rs.getString("FKTABLE_NAME");
	        String fkColumnName = rs.getString("FKCOLUMN_NAME");
	        int fkSequence = rs.getInt("KEY_SEQ");
	        /*System.out.println("getExportedKeys(): fkTableName="+fkTableName);
	        System.out.println("getExportedKeys(): fkColumnName="+fkColumnName);
	        System.out.println("getExportedKeys(): fkSequence="+fkSequence);*/
	        tablesOfFK.add(fkTableName);
	        columnNamesOfFK.add(fkColumnName);
	        
	      }
	    listOfInfo.add(tablesOfFK);
	    listOfInfo.add(columnNamesOfFK);
	    return listOfInfo;
		
	}
	//get all local foreign keys of a particular table
	
public List<List<String>> getImportedKeys(Connection conn,String tableName) throws SQLException{
		
	    DatabaseMetaData meta = conn.getMetaData();
	    ResultSet rs = meta.getImportedKeys(null, null, tableName);
	    List<List<String>> listOfInfo= new ArrayList<>();
	    List<String> tablesOfFK= new ArrayList<>();
	    List<String> columnNamesOfFK= new ArrayList<>();
	    while (rs.next()) {
	    	// check what string to get the table that has the foreign key
	        String fkTableName = rs.getString("FKTABLE_NAME");
	        String fkColumnName = rs.getString("FKCOLUMN_NAME");
	        int fkSequence = rs.getInt("KEY_SEQ");
	        /*System.out.println("getExportedKeys(): fkTableName="+fkTableName);
	        System.out.println("getExportedKeys(): fkColumnName="+fkColumnName);
	        System.out.println("getExportedKeys(): fkSequence="+fkSequence);*/
	        tablesOfFK.add(fkTableName);
	        columnNamesOfFK.add(fkColumnName);
	      }
	    listOfInfo.add(tablesOfFK);
	    listOfInfo.add(columnNamesOfFK);
	    return listOfInfo;
		
	}
	
	

}
