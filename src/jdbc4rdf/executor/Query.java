package jdbc4rdf.executor;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;


public class Query {
	
	/**
	 * Name of the query. This name is usually equal to the filename 
	 * of the query before it was translated to SQL and added to a 
	 * composite query file
	 */
	protected String queryName = "";
	
	/**
	 * The complete SQL query as string (placeholders will be removed in the constructor)
	 */
	protected String query = "";
	
	/**
	 * Statistics / Information about the tables which the query could use. The top-most
	 * table is the optimal choice
	 */
	protected String statistic = "";
	
	/**
	 * Mapping between placeholders and table names
	 */
	protected HashMap<String, Table> tables = null;
	
	
	final static Logger logger = Logger.getLogger(Query.class);
	
	/**
	 * General constructor of a query
	 * @param queryName String
	 * @param query String
	 * @param statistic String
	 * @param tables HashMap <String, Table> 
	 */
	public Query(String queryName, String query, String statistic, HashMap<String, Table> tables) {
		this.queryName = queryName;
		this.query = query;
		this.statistic = statistic;
		this.tables = tables;
		
		logger.debug("Processing query named " + this.queryName);
		
		// replace place holders in query with statistics entry
		Iterator<String> keys = this.tables.keySet().iterator();
		while (keys.hasNext()) {
			String tName = keys.next();
			Table t = this.tables.get(tName);
			
			
			// the path is the actual table if the query is of type extVP
			// x/x SS tables dont get created, so these have to get converted to vp
			// NOTE: These entries shouldnt be in the statisctis in the first place
			// ALSO NOTE: Converting to VP might not be optimal
			String[] tPath = t.tablePath.split("/");
			String actualTable = t.tablePath.replaceAll("[:]|[#]|[-]|[/]|[.]|[_]|[<]|[>]","");
			String relType = t.tableType.toUpperCase();
			if (tPath.length > 1) {
				if (tPath[0].equals(tPath[1])) {
					// Convert to VP
					actualTable = tPath[0];
					relType = "VP";
				} 
			}

			
			if (!(relType.equals("TT") || relType.equals("VP"))) {
				actualTable = relType + actualTable;
			}
			
			this.query = this.query.replace(tName, actualTable);
			logger.debug(" Replaced " + tName + " with " + actualTable);
		}
			
		
	}
	
}
