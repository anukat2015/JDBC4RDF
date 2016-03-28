package jdbc4rdf.loader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.core.sql.SQLWrapper;
import jdbc4rdf.loader.io.LoaderStatistics;


public abstract class SQLDataLoader extends SQLWrapper {

	private String dataFile = "";
	
	/**
	 * Storage for the stats which are required for the query
	 * translator
	 */
	private final LoaderStatistics stats = new LoaderStatistics();
	
	
	protected final String TT_NAME = "triples";
	
	private final TypeDetector typeChecker = new BSBMTypeDetector();
	
	
	public SQLDataLoader(LoaderConfig loaderConf) {
		super(loaderConf);
		
		this.dataFile = loaderConf.getDatafile();
	}

	
	
	
	@Override
	protected void loadData(Connection conn) throws Exception {
		// stmt exec
		
		/*
		 * Triple Table
		 */
		
		// also work with prepareStatement method to improve performance
		int tripleCount = createTripleTable(conn);
		
		/*
		 * Vertical Partitioning
		 */
		
		// if dataset_type=vp
		int predCount = createVP(conn);
		// else: retrieve the predicate count differently
		
		stats.setDatasetSize(tripleCount);
		stats.setPredicateCount(predCount);
		
		stats.closeFile(true);
		
		/*
		 * Extended Vertical Partitioning
		 */
		
		// if dt = so
		createExtVP(conn, "SO");
		// if dt = os
		createExtVP(conn, "OS");
		// if dt = ss
		createExtVP(conn, "SS");
		
		// createExtVP
		// newFile
		// closeFile(false)
		
	}
	
	
	
	private ResultSet getRelatedPredicates(String pred, String relType) {
		
		// TODO: Not yet implemented
		
		return null;
	}
	
	
	private void createExtVP(Connection conn, String relType) throws SQLException {
		// Helper.createDirInHDFS(Settings.extVpDir+relType)
		stats.newFile(relType);
		
		// retrieve all predicates from the dataset (distinct)
		String pSql = getPredicatesSql();
		PreparedStatement predStmt = prepareStatement(conn, pSql);
		predStmt.setString(1, TT_NAME);
		ResultSet plistRs = predStmt.executeQuery();
		
		// for each predicate
		while (plistRs.next()) {
			String pred1 = Helper.getPartName(plistRs.getString(1));
			ResultSet relPred = getRelatedPredicates(pred1, relType);
			
			close(relPred);
		}
		
		close(plistRs);
		
		close(predStmt);
		
		stats.closeFile(false);
		
	}
	
	
	
	/**
	 * Creates vertical partitioning tables by using the given connection
	 * @param conn
	 * @return Amount of table created (this is equal to the amount
	 * of distinct predicates in the triples dataset)
	 * @throws SQLException
	 */
	private int createVP(Connection conn) throws SQLException {
		
		stats.newFile("VP");
		
		int pcount = 0;
		
		// select distinct predicates from triples
		String pSql = getPredicatesSql();
		Statement predStmt = conn.createStatement(); 
		ResultSet plistRs = predStmt.executeQuery(pSql);
		
		// prepare filter SQL statements
		String filterSql = getPredicateFilterSql();
		PreparedStatement filterStmt = prepareStatement(conn, filterSql);
		
		// for each predicate
		while (plistRs.next()) {
			String pred = Helper.getPartName(plistRs.getString(1));
			
			// get corresponding subj/obj and create a table
			// off of it
			/*
			 * possible in hive:
			 * CREATE TABLE ext_table LOCATION '/user/XXXXX/XXXXXX' 
			 * AS SELECT * from managed_table;
			 * choose a different approach for improved compatibility
			 * => Do it via 2 statements instead of 1
			 */
			
			// drop vp
			runStaticSql(conn, getDropSql(pred));
			
			// TODO: HANDLE TYPES!!
			int[] types = detectBSBMTypes(pred);
					
			// create vp
			createVP.setString(1, pred);
			createVP.executeUpdate();
			
			// get data which should be inserted
			filterStmt.setString(1, pred);
			ResultSet filtered = filterStmt.executeQuery();
			
			// table name of vp table
			insertVP.setString(1, pred);
			
			int vpSize = 0;
			while (filtered.next()) {
				// // filterStmt.setObject(pos, val type_AS_INT)
				// http://stackoverflow.com/questions/6437790/jdbc-get-the-sql-type-name-from-java-sql-type-code
				String sub = filtered.getString(1);
				String obj = filtered.getString(2);
				insertVP.setString(2, sub);
				insertVP.setString(3, obj);
				
				// insert the data
				insertVP.executeUpdate();
				vpSize++;
			}
			
			// add statistics
			stats.incSavedTables();
			stats.addVPStatistic(pred, vpSize);
			
			close(filtered);
			
			pcount++;
		}
		
		

		
		close(insertVP);
		
		close(createVP);
		
		close(dropVP);
		
		close(filterStmt);

		close(predStmt);
		
		close(plistRs);
		
		return pcount;
	}
	
	
	
	/**
	 * Creates a triple table by using the given connection
	 * @param conn
	 * @return The amount of triples added
	 * @throws SQLException
	 */
	private int createTripleTable(Connection conn) throws SQLException {
		
		int rows = -1;
		
		// remove table first
		String sql = getDropSql(TT_NAME);
		runStaticSql(conn, sql);
		
		// create triple table
		sql = getCreateTTSql();
		runStaticSql(conn, sql);
		
		
		
		// load TSV
		sql = getLoadSql(dataFile, TT_NAME);
		runStaticSql(conn, sql);

		
		// count entries
		sql = getRowCountSql(TT_NAME);
		ArrayList<String[]> res = runStaticSql(conn, sql);
		rows = Integer.parseInt(res.get(1)[0]);
		
		return rows;
	}
	
	
	
	
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
	private ArrayList<String[]> runStaticSql(Connection conn, String sql, boolean storeRes) throws SQLException {
		Statement stmt = conn.createStatement();
		
		boolean hasRes = stmt.execute(sql);
		
		ArrayList<String[]> result = new ArrayList<String[]>();
		
		// retrieve the result
		if (hasRes && storeRes) {
			ResultSet rs = stmt.getResultSet();
			result = super.storeResultSet(rs);
		}
		
		close(stmt);
		
		return result;
		
	}
	
	
	/**
	 * Comfort function for execution a SQL statement which has 
	 * no parameters / can not be executed as a prepared statement.
	 * If there is a result, this function will store it in a list
	 * and return it
	 * @param conn
	 * @param sql
	 * @return The result set returned by the execute function if
	 * there is one. The first entry of the list will be the 
	 * table header, all following entries will be the content.
	 * @throws SQLException
	 */
	private ArrayList<String[]> runStaticSql(Connection conn, String sql) throws SQLException {
		return runStaticSql(conn, sql, true);
	}
	
	
	private PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException {
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		
		return prepStmt;
	}



	private String getCreateVPSql(String vpTableName) {
		return getCreateVPSql(vpTableName, "string", "string");
	}
	
	protected abstract String getCreateVPSql(String vpTableName, String subjType, String objType);
	
	
	protected abstract String getVPInsertSql(String vpName);
	
	
	/**
	 * 
	 * @return Correct SQL Statement which retrieves all (distinct)
	 * predicates in the triples table
	 */
	protected abstract String getPredicatesSql();
	
	
	protected abstract String getPredicateFilterSql();
	
	/**
	 * 
	 * @return A sql statement which drops a given table. The table should be
	 * marked with a ?
	 */
	protected abstract String getDropSql(String tname);
	
	/**
	 * 
	 * @return A sql statement which creates a tripleTable with
	 * the given name and the schema sub:string, pred:string, obj:string
	 */
	protected abstract String getCreateTTSql();
	
	/**
	 * 
	 * @return A sql statement which load a given (tab-separated) file 
	 * into a given table. Both should be marked by ? place holders
	 */
	protected abstract String getLoadSql(String dataFile, String tname);
	
	/**
	 * 
	 * @return  A sql statement which counts the rows of a given table
	 */
	protected abstract String getRowCountSql(String tname);
	
}
