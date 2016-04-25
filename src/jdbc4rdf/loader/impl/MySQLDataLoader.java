package jdbc4rdf.loader.impl;

import jdbc4rdf.core.config.Config;
import jdbc4rdf.loader.TypeDetector;


/**
 * DataLoader for MySql, based on the HiveDataLoader class. Some of the
 * required SQL commands are standard SQL and don't have to be defined
 * again.
 * 
 * @author Max Hofmann
 *
 */
public class MySQLDataLoader extends HiveDataLoader {

	public MySQLDataLoader(Config loadConf) {
		super(loadConf);
	}
	
	
	@Override
	protected String getCreateTTSql() {
		final String createTT = "CREATE TABLE " 
				+ super.TT_NAME + " (sub VARCHAR(1024), pred VARCHAR(1024), obj VARCHAR(" + TypeDetector.MAX_STR_SIZE + "))";
		
		return createTT;
	}
	
	
	@Override
	protected String getLoadSql(String dataFile, String table) {
		// the delimiter definitions are at the create table statement for hive
		final String loadSql = "LOAD DATA LOCAL INFILE '" + dataFile + "' INTO TABLE " + table 
				+ " FIELDS TERMINATED BY '\\t'"
				+ " LINES TERMINATED BY '\\n'";
		
		return loadSql;
	}
	
	
	@Override
	protected boolean isStringSupported() {
		return false;
	}
	
	
	/*
	 * left semi join has to implemented without the keyword
	 * 
	 * possible solutions:
	 * 1) http://stackoverflow.com/questions/21738784/difference-between-inner-join-and-left-semi-join%22
	 * 
	 * SELECT name
	 * FROM table_1 a
	 *     LEFT SEMI JOIN table_2 b ON (a.name=b.name)
	 * 
	 * SELECT name
	 * FROM table_1 a
	 * WHERE EXISTS(
	 * 		SELECT * FROM table_2 b WHERE (a.name=b.name))

	 * 
	 * 
	 * 2) https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Joins
	 * SELECT a.key, a.val
	 * FROM a 
	 * 		LEFT SEMI JOIN b ON (a.key = b.key)
	 * 
	 * SELECT a.key, a.value
	 * FROM a
	 * WHERE a.key in
	 * 		(SELECT b.key FROM B);
	 * 
	 * => Will use 1) because it is closer to the original
	 */
	
	
	@Override
	protected String getLeftJoinSql(String vpName, String relType) {
		String sql = "SELECT DISTINCT pred FROM " + this.TT_NAME + " t1 "
				+ "WHERE EXISTS ( SELECT * FROM " + vpName + " t2 "
				+ "WHERE ";
		
		// add join condition
		if (relType.equals(RELTYPE_SS)) {
			sql += "(t1.sub=t2.sub)";
			
		} else if (relType.equals(RELTYPE_OS)) {
			sql += "(t1.sub=t2.obj)";
			
		} else if (relType.equals(RELTYPE_SO)) {
			sql += "(t1.obj=t2.sub)";
		}
		
		sql += " )";
		
		return sql;
	}



	@Override
	protected String getExtVpSQLcommand(String pred1, String pred2,
			String relType) {
		String sql = "SELECT t1.sub AS sub, t1.obj AS obj "
				+ "FROM " + pred1 + " t1 "
				+ "WHERE EXISTS ( SELECT * FROM " + pred2 + " t2 "
				+ "WHERE ";

		// add join condition
		if (relType.equals(RELTYPE_SS)) {
			sql += "(t1.sub=t2.sub)";

		} else if (relType.equals(RELTYPE_OS)) {
			sql += "(t1.obj=t2.sub)";

		} else if (relType.equals(RELTYPE_SO)) {
			sql += "(t1.sub=t2.obj)";
		}
				
		sql += " )";
		
		return sql;
	}
	
	
	@Override
	protected String getDelimiter() {
		return "_";
	}
	
	

}
