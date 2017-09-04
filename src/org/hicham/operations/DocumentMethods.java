package org.hicham.operations;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

public class DocumentMethods {
	
	public void makeCollection(String DBName, String collectionName) {
		Mongo mongo = null;
        DB db=null;
        DBCollection collection=null;
        try {
            mongo = new Mongo("localhost", 27017);
        } catch (Exception e) {
            e.printStackTrace();
        }
        db=mongo.getDB(DBName);
        //make collection 
        collection = db.getCollection(collectionName);
        
	}
	public void mappingTableToDocument(List<Map<String, Object>> rows, Connection conn, String tableName)throws SQLException {
        	DatabaseMetaData meta=conn.getMetaData();
        	ResultSet rs= meta.getColumns(null, null,tableName, null);


	}
	

}
