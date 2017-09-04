package org.hicham.operations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TableConstraints {
	
	public static final int UPPERLIMIT=100;
	
	//if the table has no foreign keys inside, it should map into a document
	//but first it must check all the other tables that have its PK as FK
    //and then figure out if it can embed or reference the data in that table
	
	//get the tables the table references
	public List<String> checkTables(String tableName) throws SQLException {
		Operation operation = new Operation();
		Connection conn= operation.connectToDB();
		return operation.getExportedKeys(conn, tableName).get(0);
		
	}
	//get the keys the table references
	public List<String> checkKeys(String tableName) throws SQLException {
		Operation operation = new Operation();
		Connection conn= operation.connectToDB();
		return operation.getExportedKeys(conn, tableName).get(1);
		
	}
	//check if the data is many or few 
	//if it's few then join the tables and embed in the mapping 
	//if it's many then reference the table.
	
	// count the records of a particular table 
	public int countTableRecords(Connection conn,String tableName) throws SQLException {
		 Statement stmt = conn.createStatement();
	     String sql = "SELECT COUNT FROM "+tableName;
	     ResultSet rs = stmt.executeQuery(sql);
		 return rs.getInt(0);
	}
	
	// 
	public void operationBegin(Connection conn) throws SQLException {
		Operation operation= new Operation();
		List<String>tableNames=operation.tableNames(conn);
		for (int i = 0; i < tableNames.size(); i++) {
			
			List<String> referencedTables= checkTables(tableNames.get(i));
			Set<String>  referencedTablesSet= new HashSet<>();
			List<String> referencedKeys= checkKeys( tableNames.get(i));
			
			for (Iterator iterator = referencedTables.iterator(); iterator.hasNext();) {
				String string = (String) iterator.next();
				referencedTablesSet.add(string);
			}
			for(int j=0 ; j<referencedTables.size();j++) {
				if (countTableRecords(conn, referencedTables.get(j))<UPPERLIMIT
						&& !tableNames.get(i).equals(referencedTables.get(j))) {
					// join the two tables and make a document from the table
					
					
				}
				if (countTableRecords(conn, referencedTables.get(j))>UPPERLIMIT 
						&& !tableNames.get(i).equals(referencedTables.get(j))) {
					// link the tables
					
				}
			}
			int count =countTableRecords(conn, tableNames.get(i));
		}
	}
	
	public void joiningData() {
		
	}
	
	public void linkingData() {
		
	}
}
