package jdbc4rdf.loader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
	
	
	private final String TT_NAME = "triples";
	
	
	
	
	public SQLDataLoader(LoaderConfig loaderConf) {
		super(loaderConf);
		
		this.dataFile = loaderConf.getDatafile();
	}

	
	
	
	@Override
	protected void loadData(Connection conn) throws Exception {
		// stmt exec
		// also work with prepareStatement method to improve performance
		int tripleCount = createTripleTable(conn);
		
		int predCount = createVP(conn);
		
		stats.setDatasetSize(tripleCount);
		stats.setPredicateCount(predCount);
		
		stats.closeFile(true);
		
		// createExtVP
		// newFile
		// closeFile(false)
		
	}
	
	
	private int createVP(Connection conn) throws SQLException {
		
		stats.newFile("VP");
		
		
		int pcount = 0;
		
		// select distinct predicates from triples
		String pSql = getPredicatesSql();
		PreparedStatement predStmt = prepareStatement(conn, pSql);
		predStmt.setString(1, TT_NAME);
		ResultSet plistRs = predStmt.executeQuery();
		
		// prepare filter SQL statements
		String filterSql = getPredicateFilterSql();
		PreparedStatement filterStmt = prepareStatement(conn, filterSql);
		
		// prepare drop table statement for given predicate
		String dropVPSql = getDropSql();
		PreparedStatement dropVP = prepareStatement(conn, dropVPSql);
		
		// prepare create table statement for given predicate
		String createVPSql = getCreateVPSql();
		PreparedStatement createVP = prepareStatement(conn, createVPSql);
		
		// prepare vp insert SQL statements
		String vpInsertSql = getVPInsertSql();
		PreparedStatement insertVP = prepareStatement(conn, vpInsertSql);
		
		// for each predicate
		while (plistRs.next()) {
			String pred = Helper.cleanPredicate(plistRs.getString(1));
			
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
			dropVP.setString(1, pred);
			dropVP.executeUpdate();
			
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
		String dropSql = getDropSql();
		PreparedStatement dropTable = prepareStatement(conn, dropSql);
		dropTable.setString(1, TT_NAME);
		dropTable.executeUpdate();
		
		close(dropTable);
		
		
		// create table
		String ctSql = getCreateTTSql();
		PreparedStatement ctStmt = prepareStatement(conn, ctSql);
		ctStmt.setString(1, TT_NAME);
		ctStmt.executeUpdate();
		
		close(ctStmt);
		
		
		// load tsv
		String loadSql = getLoadSql();
		PreparedStatement st = prepareStatement(conn, loadSql);
		st.setString(1, dataFile);
		st.setString(2, TT_NAME);
		st.executeUpdate();
		
		close(st);
		
		
		// count entries
		String rcSql = getRowCountSql();
		PreparedStatement countTriples = prepareStatement(conn, rcSql);
		countTriples.setString(1, TT_NAME);
		ResultSet rs = countTriples.executeQuery();
		rows = rs.getInt(1);
		
		close(rs);
		close(countTriples);
		
		return rows;
	}
	
	
	private PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException {
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		
		return prepStmt;
	}



	protected abstract String getCreateVPSql();
	
	protected abstract String getVPInsertSql();
	
	protected abstract String getPredicatesSql();
	
	protected abstract String getPredicateFilterSql();
	
	/**
	 * 
	 * @return A sql statement which drops a given table. The table should be
	 * marked with a ?
	 */
	protected abstract String getDropSql();
	
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
	protected abstract String getLoadSql();
	
	/**
	 * 
	 * @return  A sql statement which counts the rows of a given table
	 */
	protected abstract String getRowCountSql();
	
}
