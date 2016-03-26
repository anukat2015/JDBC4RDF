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
		String createTT = "CREATE TABLE ? (sub string, pred string, obj string)"
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
		String loadSql = "LOAD DATA LOCAL INPATH '?' INTO TABLE ? ";
		return loadSql;
	}

	@Override
	protected String getRowCountSql() { 
		return "SELECT count(*) FROM ?";
	}

	


	
}
