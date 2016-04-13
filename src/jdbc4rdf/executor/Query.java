package jdbc4rdf.executor;

import java.util.HashMap;

public class Query {
	
	protected String queryName = "";
	protected String query = "";
	protected String statistic = "";
	protected HashMap<String, Table> tables = null;
	
	/**
	 * General constructor of a query
	 * @param queryName String
	 * @param query String
	 * @param statistic String
	 * @param tables HashMap <String, Table> 
	 */
	public Query(String queryName, String query, String statistic, HashMap<String, Table> tables) {
		super();
		this.queryName = queryName;
		this.query = query;
		this.statistic = statistic;
		this.tables = tables;
	}
	
}
