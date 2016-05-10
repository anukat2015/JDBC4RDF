package jdbc4rdf.executor;

import java.util.HashMap;
import java.util.Iterator;


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
		this.queryName = queryName;
		this.query = query;
		this.statistic = statistic;
		this.tables = tables;
		
		
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
			System.out.println(" Replaced " + tName + " with " + actualTable);
		}
			
		
	}
	
}
