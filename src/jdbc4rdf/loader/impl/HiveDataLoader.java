package jdbc4rdf.loader.impl;

import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.loader.SQLDataLoader;


public class HiveDataLoader extends SQLDataLoader {

	public HiveDataLoader(LoaderConfig loadConf) {
		super(loadConf);
	}

	
	
	
	
	@Override
	protected String getDropSql(String table) {
		return "DROP TABLE IF EXISTS " + table;
	}

	@Override
	protected String getCreateTTSql() {
		final String createTT = "CREATE TABLE " + super.TT_NAME + " (sub string, pred string, obj string)"
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
		return "SELECT sub, obj FROM triples WHERE pred = '?'";
	}

	@Override
	protected String getPredicatesSql() {
		return "SELECT DISTINCT pred from " + super.TT_NAME;
	}

	@Override
	protected String getVPInsertSql(String vpName) {
		final String insertSql = "INSERT INTO " + vpName + " VALUES('?', '?')";
		
		return insertSql;
	}

	@Override
	protected String getCreateVPSql() {
		final String createSql = "CREATE TABLE ? (sub string, obj string)";
		
		return createSql;
	}




	
}
