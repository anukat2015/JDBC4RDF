package jdbc4rdf.executor;

import java.util.HashMap;

public class Query {
	
	protected String queryName = "";
	protected String query = "";
	protected String statistic = "";
	protected HashMap<String, Table> tables = null;
	
	
	public Query(String queryName, String query, String statistic, HashMap<String, Table> tables) {
		super();
		this.queryName = queryName;
		this.query = query;
		this.statistic = statistic;
		this.tables = tables;
	}


	public String getQueryName() {
		return queryName;
	}


	public void setQueryName(String queryName) {
		this.queryName = queryName;
	}


	public String getQuery() {
		return query;
	}


	public void setQuery(String query) {
		this.query = query;
	}


	public String getStatistic() {
		return statistic;
	}


	public void setStatistic(String statistic) {
		this.statistic = statistic;
	}


	public HashMap<String, Table> getTables() {
		return tables;
	}


	public void setTables(HashMap<String, Table> tables) {
		this.tables = tables;
	}
	
	

}
