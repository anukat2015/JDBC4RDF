package jdbc4rdf.loader.impl;

import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.loader.SQLDataLoader;


public class HiveDataLoader extends SQLDataLoader {

	public HiveDataLoader(LoaderConfig loadConf) {
		super(loadConf);
	}

	
	
	
	
	@Override
	protected String getDropSql() {
		return "DROP TABLE IF EXISTS ?";
	}

	@Override
	protected String getCreateTTSql() {
		final String createTT = "CREATE TABLE ? (sub string, pred string, obj string)"
				+ " ROW FORMAT DELIMITED"
		        + " FIELDS TERMINATED BY '\t'"
		        + " LINES TERMINATED BY '\n'";
		
		return createTT;
	}
	
	@Override
	protected String getLoadSql() {
		/*
		 * for skipping the first row:
		 * row format delimited fields terminated BY '\t' lines terminated BY '\n' 
			tblproperties("skip.header.line.count"="1"); 
		 */
		final String loadSql = "LOAD DATA LOCAL INPATH '?' INTO TABLE ? ";
		
		return loadSql;
	}

	@Override
	protected String getRowCountSql() { 
		return "SELECT count(*) FROM ?";
	}

	@Override
	protected String getPredicateFilterSql() {
		return "SELECT sub, obj FROM triples WHERE pred = '?'";
	}

	@Override
	protected String getPredicatesSql() {
		return "SELECT DISTINCT pred from ?";
	}

	@Override
	protected String getVPInsertSql() {
		final String insertSql = "INSERT INTO ? VALUES('?', '?')";
		
		return insertSql;
	}

	@Override
	protected String getCreateVPSql() {
		final String createSql = "CREATE TABLE ? (sub string, obj string)";
		
		return createSql;
	}




	
}
