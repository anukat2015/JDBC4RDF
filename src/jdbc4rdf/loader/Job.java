package jdbc4rdf.loader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public abstract class Job {

	public abstract void runJob(Connection conn) throws Exception ;
	
	/**
	 * Comfort function for execution a SQL statement which has 
	 * no parameters / can not be executed as a prepared statement
	 * @param conn
	 * @param sql
	 * @param storeRes set to true iff the result should be copied
	 * in a List<String[]>
	 * This parameter has no effect if there is no result
	 * @return The result set returned by the execute function if
	 * there is one. The first entry of the list will be the 
	 * table header, all following entries will be the content.
	 * @throws SQLException
	 */
	/*public ArrayList<String[]> runSql(Connection conn, String sql, boolean storeRes) throws SQLException {
		Statement stmt = conn.createStatement();
		
		boolean hasRes = stmt.execute(sql);
		
		ArrayList<String[]> result = new ArrayList<String[]>();
		
		// retrieve the result, make sure it always gets closes
		if (hasRes) {
			ResultSet rs = stmt.getResultSet();
			if (storeRes) {
				// close() happens in store() method
				result = storeResultSet(rs);
			} else {
				// close result set manually
				close(rs);
			}
		}
		
		close(stmt);
		
		return result;
		
	}
	
	
	protected ArrayList<String[]> storeResultSet(ResultSet rs) throws SQLException {
		ArrayList<String[]> rtable = new ArrayList<String[]>();
		
		ResultSetMetaData rsmd = rs.getMetaData();
		int ccount = rsmd.getColumnCount();
		*/
		/*
		 * TEST:
		 * SELECT preis as pxy
		 * ColumnName = preis
		 * ColumnLabel = pxy
		 * SELECT preis
		 * ColumnName = preis
		 * ColumnLabel = preis
		 */
		/*
		// header
		String[] head = new String[ccount];
		for (int i = 1; i <= ccount; i++) { 
			head[i-1] = rsmd.getColumnLabel(i);
		}
		rtable.add(head);
		
		// table content
		while (rs.next()) {
			String[] row = new String[ccount];
		    for (int i = 1; i <= ccount; i++) {
		    	row[i-1] = rs.getString(i);
		    }
		    rtable.add(row);
		}
		
		// close the result set
		close(rs);

		return rtable;
	}
	
	
	
	

	protected void close(AutoCloseable ac) {
		try {
			if (ac != null) {
				// also commit here?
				ac.close();
			} else {
				System.out.println("[WARNING] close() "
						+ "AutoClosable \"ac\" can not be "
						+ "closed because it is already null");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	*/
	
	
}
