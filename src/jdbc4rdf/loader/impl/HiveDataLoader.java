package jdbc4rdf.loader.impl;

import jdbc4rdf.core.config.Config;
import jdbc4rdf.loader.SQLDataLoader;


public class HiveDataLoader extends SQLDataLoader {

	public HiveDataLoader(Config loadConf) {
		super(loadConf);
	}

	
	
	@Override
	protected String getDropSql(String table) {
		return "DROP TABLE IF EXISTS " + table;
	}

	@Override
	protected String getCreateTTSql() {
		final String createTT = "CREATE TABLE " 
				+ super.TT_NAME + " (sub string, pred string, obj string)"
				+ " ROW FORMAT DELIMITED"
		        + " FIELDS TERMINATED BY '\t'"
		        + " LINES TERMINATED BY '\n'";
		
		return createTT;
	}
	
	@Override
	protected String getLoadSql(String dataFile, String table) {
		/*
		 * for skipping the first row:
		 * row format delimited fields terminated BY '\t' lines terminated BY '\n' 
			tblproperties("skip.header.line.count"="1"); 
		 */
		final String loadSql = "LOAD DATA LOCAL INPATH '" + dataFile + "' INTO TABLE " + table + " ";
		
		return loadSql;
	}

	@Override
	protected String getRowCountSql(String table) { 
		return "SELECT count(*) FROM " + table;
	}

	@Override
	protected String getPredicateFilterSql() {
		return "SELECT sub, obj FROM " + super.TT_NAME + " WHERE pred = '?'";
	}

	@Override
	protected String getPredicatesSql() {
		return "SELECT DISTINCT pred from " + super.TT_NAME;
	}

	@Override
	protected String getInsertSql(String tName, int colCount) {
		String insertSql = "INSERT INTO " + tName + " VALUES(";
		
		for (int i = 0; i < colCount; i++) {
			if (i > 0) insertSql += ",";
			insertSql += " ?";
		}
			
		
		insertSql += " )";
		
		return insertSql;
	}

	@Override
	protected String getCreateVPSql(String vpTableName, String subjType, String objType) {
		final String createSql = "CREATE TABLE " + vpTableName 
				+ " (sub " + subjType + ", obj " + objType + ")";
		
		return createSql;
	}

	
	
	
	@Override
	protected boolean isStringSupported() {
		return true;
	}



	@Override
	protected String getLeftJoinSql(String vpName, String relType) {
		String sql = "SELECT DISTINCT pred FROM " + this.TT_NAME + " t1 "
				+ "LEFT SEMI JOIN " + vpName + " t2 "
				+ "ON ";
		
		// add join condition
		if (relType.equals(RELTYPE_SS)) {
			sql += "(t1.sub=t2.sub)";
			
		} else if (relType.equals(RELTYPE_OS)) {
			sql += "(t1.sub=t2.obj)";
			
		} else if (relType.equals(RELTYPE_SO)) {
			sql += "(t1.obj=t2.sub)";
		}
		
		return sql;
	}



	@Override
	protected String getExtVpSQLcommand(String pred1, String pred2,
			String relType) {
		String sql = "SELECT t1.sub AS sub, t1.obj AS obj "
				+ "FROM " + pred1 + " t1 "
				+ "LEFT SEMI JOIN " + pred2 + " t2 "
				+ "ON ";

		// add join condition
		if (relType.equals(RELTYPE_SS)) {
			sql += "(t1.sub=t2.sub)";

		} else if (relType.equals(RELTYPE_OS)) {
			sql += "(t1.obj=t2.sub)";

		} else if (relType.equals(RELTYPE_SO)) {
			sql += "(t1.sub=t2.obj)";
		}
				
		return sql;
	}



	@Override
	protected String getCreateDbSql(String dbName) {
		return ("CREATE DATABASE IF NOT EXISTS " + dbName);
	}



	
}
