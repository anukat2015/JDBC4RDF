package jdbc4rdf.loader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import jdbc4rdf.core.config.Config;
import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.core.sql.SQLWrapper;
import jdbc4rdf.loader.io.LoaderStatistics;


public abstract class SQLDataLoader extends SQLWrapper {

	/*
	 * This delimiter should be used as an alternative
	 * to the "/" delimiter in the original implementation
	 */
	private String DELIM  = "____X____";
	
	/**
	 * this option is for debug purposes and shortens vp table names.
	 * however, this does not work for extVP because the
	 * extVp creation method will not be able to find
	 * the tables
	 */
	private final boolean SHORTEN_TABLENAMES = false;
	
	private String dataFile = "";
	
	private float scaleUB = 1;
	
	/**
	 * Storage for the stats which are required for the query
	 * translator
	 */
	private final LoaderStatistics stats = new LoaderStatistics();
	
	
	protected final String TT_NAME = "triples";
	
	protected final String RELTYPE_SS = "SS";
	protected final String RELTYPE_OS = "OS";
	protected final String RELTYPE_SO = "SO";
	
	
	// TODO: add bm-type as a setting
	private final TypeDetector typeChecker = new BSBMTypeDetector();
	
	
	public SQLDataLoader(Config loaderConf) {
		super(loaderConf);
		
		
		// treat it as a loaderConfig instance in order to get
		// all arguments
		
		this.dataFile = ((LoaderConfig) loaderConf).getDatafile();
		
		this.scaleUB = ((LoaderConfig) loaderConf).getScaleUB();
	}

	
	
	
	@Override
	protected void loadData(Connection conn) throws Exception {
		/*
		 * Create database if not exists
		 */
		
		initDb(conn);
		
		/*
		 * Triple Table
		 */
		
		// also work with prepareStatement method to improve performance
		int tripleCount = createTripleTable(conn);
		
		stats.setDatasetSize(tripleCount);
		
		/*
		 * Vertical Partitioning
		 */
		
		// if dataset_type=vp
		int predCount = createVP(conn);
		// else: retrieve the predicate count differently
		
		stats.setPredicateCount(predCount);
		
		stats.closeFile(true);
		
		/*
		 * Extended Vertical Partitioning
		 */
		
		if (!SHORTEN_TABLENAMES) {
			// if dt = so
			createExtVP(conn, RELTYPE_SO);
			// if dt = os
			createExtVP(conn, RELTYPE_OS);
			// if dt = ss
			createExtVP(conn, RELTYPE_SS);
		}
		// createExtVP
		// newFile
		// closeFile(false)
		
	}


	
	private void initDb(Connection conn) throws Exception {
		final String dbName = this.conf.getDbName();
		
		// default database is always there
		if (!dbName.isEmpty()) {
			String sql = getCreateDbSql(dbName);
			runStaticSql(conn, sql, false);
		}
	}

	
	
	private void createExtVP(Connection conn, String relType) throws SQLException {
		// Helper.createDirInHDFS(Settings.extVpDir+relType)
		stats.newFile(relType);

		// retrieve all predicates from the dataset (distinct)
		String pSql = getPredicatesSql();
		Statement predStmt = conn.createStatement();
		ResultSet plistRs = predStmt.executeQuery(pSql);

		// for each predicate
		while (plistRs.next()) {
			String pred1 = Helper.getPartName(plistRs.getString(1));

			// get related predicates
			Statement relPredStmt = conn.createStatement();
			ResultSet relPred = relPredStmt.executeQuery(getLeftJoinSql(pred1, relType));

			// for each related predicate
			while(relPred.next()) {
				String pred2 = Helper.getPartName(relPred.getString(1));

				int extVpTableSize = -1;

				// Don't create unnecessary tables
				if (!(relType == RELTYPE_SS && pred1.equals(pred2))) {
					String extVPSql = getExtVpSQLcommand(pred1, pred2, relType);

					// calculate size
					extVpTableSize = runStaticSql(conn, extVPSql, true).size();

					Statement extVPStmt = conn.createStatement();
					ResultSet extVPRes = extVPStmt.executeQuery(extVPSql);


					if (extVpTableSize < (stats.getVPTableSize(pred1) * this.scaleUB) ) {

						// - omit directory check -

						// save the extVP table
						String tableName =  relType + DELIM 
									+ pred1 + DELIM + pred2;
						
						resultSetToTable(conn, extVPRes, tableName);
						
						stats.incSavedTables();

					} else {
						stats.incUnsavedNonEmptyTables();
					}

					close(extVPRes);

					close(extVPStmt);
				} else {
					extVpTableSize = stats.getVPTableSize(pred1);
				}
				
				
				// write statistics
				stats.addExtVPStatistic(pred1, pred2, extVpTableSize);
				
			}


			close(relPred);

			close (relPredStmt);
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
		
		int tcount = 0;
		
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
			String pred = plistRs.getString(1);
			
			String tname = Helper.getPartName(pred);
			if (SHORTEN_TABLENAMES) {
				tname = "vptable_" + tcount;
				tcount++;
			}
			
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
			runStaticSql(conn, getDropSql(tname), false);
			
			// detect type
			final int otype = typeChecker.detectObjectType(pred);
			final String otypeStr = typeChecker.getTypeName(otype, this.isStringSupported());
			final boolean isTimestamp = (otype == Types.TIMESTAMP);
			// max. uri length
			String stypeStr = "VARCHAR(" + 1024 + ")";
			if (isStringSupported()) {
				stypeStr = "STRING";
			}
			
			// create VP
			runStaticSql(conn, getCreateVPSql(tname, stypeStr, otypeStr), false);

			// get data which should be inserted
			filterStmt.setString(1, pred);
			ResultSet filtered = filterStmt.executeQuery();

			// insert into table
			String insSql = this.getInsertSql(tname, 2);
			PreparedStatement insertVP = prepareStatement(conn, insSql); 
			
			int vpSize = 0;
			while (filtered.next()) {
				// filterStmt.setObject(pos, val type_AS_INT)
				String sub = filtered.getString(1);
				String obj = filtered.getString(2);
				
				obj = Helper.cleanObject(obj, isTimestamp);
				
				insertVP.setString(1, sub);
				insertVP.setObject(2, obj, otype);
				// there is also a length parameter which might be useful for decimal/numerical
				// but these values do not exist here
				
				// insert the data
				insertVP.executeUpdate();
				vpSize++;
			}
			
			// close insertVP prepared statement
			close(insertVP);
			
			// add statistics
			stats.incSavedTables();
			stats.addVPStatistic(pred, vpSize);
			
			close(filtered);
			
			pcount++;
		}
		
		
		// close everything
		
		close(filterStmt);
		
		close(plistRs);
		
		
		// return amount of predicates / vp tables
		
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
		runStaticSql(conn, sql, false);
		
		// create triple table
		sql = getCreateTTSql();
		runStaticSql(conn, sql, false);
		
		
		
		// load TSV
		sql = getLoadSql(dataFile, TT_NAME);
		runStaticSql(conn, sql, false);

		
		// count entries
		sql = getRowCountSql(TT_NAME);
		ArrayList<String[]> res = runStaticSql(conn, sql);
		rows = Integer.parseInt(res.get(1)[0]);
		
		return rows;
	}
	
	
	
	
	private void resultSetToTable(Connection conn, ResultSet rs, String tableName) throws SQLException {
		// adapted from
		// http://stackoverflow.com/questions/11268057/how-to-create-table-based-on-jdbc-result-set
		
		ResultSetMetaData rsmd = rs.getMetaData();
		
		final int COLUMNS = rsmd.getColumnCount();
		
		/*
		 * Step 1 - Create the table
		 */
		
		
		String createSql = "CREATE TABLE " + tableName + " (";
		
		for (int i = 1; i <= COLUMNS; i++) {
			if (i > 1) createSql += ",";
			
			createSql += " " + rsmd.getColumnLabel(i)  + " " + rsmd.getColumnTypeName(i);
			
			// check if there is a precision value
			int precision = rsmd.getPrecision(i);
		    if ( precision != 0 ) {
		    	createSql += "(" + precision + ")";
		    }
		}
		
		createSql += " )";
		
		// finally, create the table
		
		runStaticSql(conn, createSql, false);
		
		
		/*
		 * Step 2 - Insert data
		 */
		
		// prepare a insert statement
		String insertSql = getInsertSql(tableName, COLUMNS);
		PreparedStatement insertStmt = prepareStatement(conn, insertSql);
		
		// for each row...
		while(rs.next()) {
			for (int i = 1; i <= COLUMNS; i++) {
				insertStmt.setObject(i, rs.getObject(i), rsmd.getColumnType(1));
			}
			// insert this row
			insertStmt.executeUpdate();
		}
		
		insertStmt.close();
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
		
		// retrieve the result, make sure it always gets closes
		if (hasRes) {
			ResultSet rs = stmt.getResultSet();
			if (storeRes) {
				// close() happens in store() method
				result = super.storeResultSet(rs);
			} else {
				// close result set manually
				close(rs);
			}
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

	
	protected abstract String getCreateVPSql(String vpTableName, String subjType, String objType);
	
	
	protected abstract String getInsertSql(String tName, int colCount);
	
	
	protected abstract String getCreateDbSql(String dbName);
	
	
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
	
	
	protected abstract String getLeftJoinSql(String vpName, String relType);
	
	
	protected abstract String getExtVpSQLcommand(String pred1, String pred2, String relType);
	
	
	/**
	 * 
	 * @return True iff the string datatype is supported
	 */
	protected abstract boolean isStringSupported();
}
