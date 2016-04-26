package jdbc4rdf.loader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import jdbc4rdf.core.config.Config;
import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.core.sql.SQLWrapper;
import jdbc4rdf.loader.io.LoaderStatistics;


public abstract class SQLDataLoader extends SQLWrapper implements DataLoader {

	/*
	 * This delimiter should be used as an alternative
	 * to the "/" delimiter in the original implementation
	 */
	private String DELIM  = "_X_";


	
	private String dataFile = "";
	
	private float scaleUB = 1;
	
	/**
	 * Storage for the stats which are required for the query
	 * translator
	 */
	private final LoaderStatistics stats = new LoaderStatistics();
	
	
	/**
	 * Time stamp for measuring execution time
	 */
	private long start = 0;
	
	protected final String TT_NAME = "triples";
	
	
	private String reltype = "";
	
	protected final String RELTYPE_SS = "SS";
	protected final String RELTYPE_OS = "OS";
	protected final String RELTYPE_SO = "SO";
	
	
	private int mode = MODE_IDLE;
	
	public static final int MODE_IDLE = -1;
	public static final int MODE_TT = 0;
	public static final int MODE_VP = 1;
	public static final int MODE_EXTVP = 2;
	
	private boolean producerFinished = false;
	
	ArrayList<JobExecuter> workerList = new ArrayList<JobExecuter>();
	
	private int idleThreads = 0;
	
	// TODO: add bm-type as a setting
	private final TypeDetector typeChecker = new BSBMTypeDetector();
	
	
	private final ConcurrentLinkedQueue<Job> jobQueue = new ConcurrentLinkedQueue<Job>();
	
	
	public SQLDataLoader(Config loaderConf) {
		super(loaderConf);
		
		// treat it as a loaderConfig instance in order to get
		// all arguments
		
		this.dataFile = ((LoaderConfig) loaderConf).getDatafile();

		this.scaleUB = ((LoaderConfig) loaderConf).getScaleUB();
	}

	
	
	private synchronized void onThreadIdle() {
		idleThreads++;
		System.out.println(idleThreads + " worker(s) finished");
		if (idleThreads >= workerList.size()) {
			// all threads are in idle mode!
			// print execution time!
			long elapsed = System.nanoTime() - start;
			printTime(elapsed);
			
			// close stats file
			stats.closeFile(mode == MODE_VP);
			
			// go to next job type
			if (mode == MODE_VP) {
				// switch to extvp and start with SO
				this.reltype = RELTYPE_SO;
				mode = MODE_EXTVP;
				//CREATE
			} else if (mode == MODE_EXTVP) {
				if (reltype.equals(RELTYPE_SO)) {
					// go to os
					this.reltype = RELTYPE_OS;
					//CREATE
				} else if (reltype.equals(RELTYPE_OS)) {
					// go to ss
					this.reltype = RELTYPE_SS;
					//CREATE
				} else if (reltype.equals(RELTYPE_SS)) {
					// all extvp tables are created
					// stop all threads
					for (int i = 0; i < workerList.size(); i++) {
						workerList.get(i).stop();
						System.out.println("Stopped " + (i+1) + " worker(s)");
					}
					mode = MODE_IDLE;
					close(conn);
					
					System.out.println("\n\n> Done!");
				}
			}
			
			
			// create extvp if required
			if (mode == MODE_EXTVP) {
				// reset flags
				producerFinished = false;
				idleThreads = 0;
				// resume all
				for (int i = 0; i < workerList.size(); i++) {
					workerList.get(i).resume();
					System.out.println("Resumed " + (i+1) + " worker(s)");
				}
				
				try {
					// reset start timestamp
					start = System.nanoTime();
					createExtVP();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
		
	}
	
	private void printTime(long nanoSeconds) {
		System.out.println("Time elapsed (h:m:s:ms)");
		// 1*10^6
		System.out.println("\t" + (nanoSeconds / 1000000.0) + " milliseconds, or:");
		
		long milliSec = (nanoSeconds / 1000000) % 1000;
		// sec = nano / 1*10^9
		// or sec = msec / 1000
		long s = nanoSeconds / 1000000000;
		long seconds = (s % 60);  
		long minutes = (s % 3600) / 60;
		long hours = s / 3600;
		
		String output = String.format("%02d:%02d:%02d:%02d", hours, minutes, seconds, milliSec);
		System.out.println("\t" + output + "\n");
		
	}
	
	@Override
	public void runSql() {
		
		/*
		 * preparation
		 */
		
		start = System.nanoTime();
		
		System.out.println("Preparing database");
		String dbname = super.conf.getDbName();
		try {
			Statement st = conn.createStatement();
			if(!isHive()){
				st.executeUpdate("DROP DATABASE IF EXISTS " + dbname);
			}else{
				st.executeUpdate("DROP DATABASE IF EXISTS " + dbname + " CASCADE");
			}
			if(!isHive()){
				conn.commit();
			}
			st.executeUpdate("CREATE DATABASE " + dbname);
			if(!isHive()){
				conn.commit();
			}
			st.execute("use " + dbname);
			if(!isHive()){
				conn.commit();
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
		long elapsed = System.nanoTime() - start;
		printTime(elapsed);
		
		/*
		 * Triple Table
		 */
		
		start = System.nanoTime();
		// also work with prepareStatement method to improve performance
		int tripleCount = 0;
		try {
			tripleCount = createTripleTable();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		elapsed = System.nanoTime() - start;
		printTime(elapsed);
		
		
		
		stats.setDatasetSize(tripleCount);
		
		
		
		// create threads
		/*
		 * mysql: 151
		 * https://dev.mysql.com/doc/refman/5.5/en/too-many-connections.html
		 * hive: max. 500 worker threads
		 * https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2
		 */
		int allowedConnections = 64;
		int logicalCores = Runtime.getRuntime().availableProcessors();
		int threadCount = Math.min(allowedConnections, Math.max(1, logicalCores - 1));
		for (int i = 0; i < threadCount; i++) {
			// create and store reference
			JobExecuter je = new JobExecuter(super.conf);
			workerList.add(je);
			// start
			Thread t = new Thread(je);
			t.start();
		}
		
		
		/*
		 * Vertical Partitioning
		 */
		
		start = System.nanoTime();
		// if dataset_type=VP
		int predCount = 0;
		try {
			predCount = createVP();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		stats.setPredicateCount(predCount);
		
		producerFinished = true;
		// stats.closeFile(true);
		
		/*
		 * Extended Vertical Partitioning
		 */

		/*
		// if dt = so
		start = System.nanoTime();
		this.reltype = RELTYPE_SO;
		createExtVP(conn);
		elapsed = System.nanoTime() - start;
		printTime(elapsed);

		// if dt = os
		start = System.nanoTime();
		this.reltype = RELTYPE_OS;
		createExtVP(conn);
		elapsed = System.nanoTime() - start;
		printTime(elapsed);

		// if dt = ss
		start = System.nanoTime();
		this.reltype = RELTYPE_SS;
		createExtVP(conn);
		elapsed = System.nanoTime() - start;
		printTime(elapsed);
		*/
		// createExtVP
		// newFile
		// closeFile(false)
		
	}


	/*
	private void initDb(Connection conn) throws Exception {
		final String dbName = this.conf.getDbName();
		
		// default database is always there
		if (!dbName.isEmpty()) {
			String sql = getCreateDbSql(dbName);
			runStaticSql(conn, sql, false);
		}
	}
	*/

	
	
	private void createExtVP() throws SQLException {
		// Helper.createDirInHDFS(Settings.extVpDir+relType)
		
		stats.newFile(this.reltype);

		
		// retrieve all predicates from the dataset (distinct)
		String pSql = getPredicatesSql();
		Statement predStmt = conn.createStatement();
		ResultSet plistRs = predStmt.executeQuery(pSql);
		
		System.out.println("Creating ExtVP (" + this.reltype + ") tables...");
		
		// for each predicate
		while (plistRs.next()) {
			// String pred1 = Helper.getPartName(plistRs.getString(1));
			String pred1 = plistRs.getString(1);
			
			Job j = new ExtVPJob(pred1);
			this.jobQueue.add(j);
			
			/*String pred1Table = Helper.getPartName(pred1);
			
			// get related predicates
			Statement relPredStmt = conn.createStatement();
			ResultSet relPred = relPredStmt.executeQuery(getLeftJoinSql(pred1Table, relType));

			// for each related predicate
			while(relPred.next()) {
				//String pred2 = Helper.getPartName(relPred.getString(1));
				String pred2 = relPred.getString(1);
				String pred2Table = Helper.getPartName(pred2);

				int extVpTableSize = -1;

				// Don't create unnecessary tables
				if (!(relType == RELTYPE_SS && pred1.equals(pred2))) {
					String extVPSql = getExtVpSQLcommand(pred1Table, pred2Table, relType);

					// calculate size
					extVpTableSize = runStaticSql(conn, extVPSql, true).size() - 1;

					Statement extVPStmt = conn.createStatement();
					ResultSet extVPRes = extVPStmt.executeQuery(extVPSql);


					if (extVpTableSize < (stats.getVPTableSize(pred1) * this.scaleUB) ) {

						// - omit directory check -

						// save the extVP table
						String tableName =  relType + DELIM 
									+ pred1Table + DELIM + pred2Table;
						
						//Drop table
						runStaticSql(conn, getDropSql(tableName), false);
						
						resultSetToTable(conn, extVPRes, tableName);
						
						stats.incSavedTables();

					} else {
						stats.incUnsavedNonEmptyTables();
					}

					close(extVPRes);

					close(extVPStmt);
				} else {
					extVpTableSize = stats.getVPTableSize(pred1Table);
				}
				
				
				// write statistics
				stats.addExtVPStatistic(pred1, pred2, extVpTableSize);
				
			}


			close(relPred);

			close (relPredStmt);*/
		}
		
		
		System.out.println("Created all ExtVP (" + this.reltype + ") jobs!");

		close(plistRs);

		close(predStmt);

		producerFinished = true;
		
		
		//stats.closeFile(false);

	}

	
	
	/**
	 * Creates vertical partitioning tables by using the given connection
	 * @return Amount of table created (this is equal to the amount
	 * of distinct predicates in the triples dataset)
	 * @throws SQLException
	 */
	private int createVP() throws SQLException {
		
		stats.newFile("VP");
		
		
		// set thread flags
		mode = MODE_VP;
		producerFinished = false;
		idleThreads = 0;
		// resume all threads
		for (int i = 0; i < workerList.size(); i++) {
			workerList.get(i).resume();
		}
		
		
		int pcount = 0;
		
		// select distinct predicates from triples
		String pSql = getPredicatesSql();
		Statement predStmt = conn.createStatement(); 
		ResultSet plistRs = predStmt.executeQuery(pSql);
		
		// prepare filter SQL statements
		//String filterSql = getPredicateFilterSql();
		//PreparedStatement filterStmt = prepareStatement(conn, filterSql);
		
		System.out.println("Creating VP tables...");
		
		// for each predicate
		while (plistRs.next()) {
			String pred = plistRs.getString(1);
			
			Job tmp = new VPJob(pred);
			this.jobQueue.add(tmp);
			
			//String tname = Helper.getPartName(pred);

			
			// get corresponding subj/obj and create a table
			// off of it
			/*
			 * possible in hive:
			 * CREATE TABLE ext_table LOCATION '/user/XXXXX/XXXXXX' 
			 * AS SELECT * from managed_table;
			 * choose a different approach for improved compatibility
			 * => Do it via 2 statements instead of 1
			 */
			/*
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
			*/
			pcount++;
		}
		
		
		// close everything
		
		//close(filterStmt);
		
		close(plistRs);
		
		System.out.println("Created all VP jobs!");
		
		// return amount of predicates / vp tables
		
		return pcount;
	}
	
	
	
	/**
	 * Creates a triple table by using the given connection
	 * @return The amount of triples added
	 * @throws SQLException
	 */
	private int createTripleTable() throws SQLException {
		
		int rows = -1;
		mode = MODE_TT;
		System.out.println("Creating triple table...");
		
		// remove table first
		String sql = getDropSql(TT_NAME);
		runStaticSql(conn, sql, false);
		if(!isHive()){
			conn.commit();
		}
		
		
		// create triple table
		sql = getCreateTTSql();
		runStaticSql(conn, sql, false);
		if(!isHive()){
			conn.commit();
		}
		
		
		// load TSV
		sql = getLoadSql(dataFile, TT_NAME);
		runStaticSql(conn, sql, false);
		if(!isHive()){
			conn.commit();
		}
		
		// count entries
		sql = getRowCountSql(TT_NAME);
		ArrayList<String[]> res = runStaticSql(conn, sql);
		rows = Integer.parseInt(res.get(1)[0]);
		
		System.out.println("Done!");
		
		return rows;
	}
	
	
	
	
	/*private void resultSetToTable(Connection conn, ResultSet rs, String tableName) throws SQLException {
		// adapted from
		// http://stackoverflow.com/questions/11268057/how-to-create-table-based-on-jdbc-result-set
		
		ResultSetMetaData rsmd = rs.getMetaData();
		
		final int COLUMNS = rsmd.getColumnCount();
		*/
		/*
		 * Step 1 - Create the table
		 */
		/*
		
		String createSql = "CREATE TABLE " + tableName + " (";
		
		for (int i = 1; i <= COLUMNS; i++) {
			if (i > 1) createSql += ",";
			
			createSql += " " + rsmd.getColumnLabel(i)  + " " + rsmd.getColumnTypeName(i);
			
			// check if there is a precision value
			int precision = rsmd.getPrecision(i);
		    if (rsmd.getColumnTypeName(i).equalsIgnoreCase("VARCHAR"))
				if ( precision != 0 ) {
			    	createSql += "(" + precision + ")";
			    }
		}
		
		createSql += " )";
		
		// finally, create the table
		runStaticSql(conn, createSql, false);
		
		*/
		/*
		 * Step 2 - Insert data
		 */
		/*
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
	}*/
	
	
	
	
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
	
	
	
	/**
	 * 
	 * @return True iff prepared statements are supported by the DB driver
	 */
	protected abstract boolean isPrepareSupported();
	
	/**
	 * 
	 * @return Delimiter character for table names (mysql: _ hive: $)
	 */
	protected abstract String getDelimiter();
	
	/**
	 * 
	 * @return True if DB driver is Hive
	 */
	protected abstract boolean isHive();
	
	
	
	
	
	
	
	
	
	
	class VPJob extends Job {

		private final String pred;
		
		private PreparedStatement filterStmt;
		
		public VPJob(String pred) {
			this.pred = pred;
			
		}
		
		
		public void setFilterStmt(PreparedStatement filterStmt) {
			this.filterStmt = filterStmt;
		}
		
		/**
		 * IMPORTANT: RUN setFilterStmt before executing this method!!!!
		 */
		@Override
		public void runJob(Connection conn) throws Exception {
			String tname = Helper.getPartName(pred, getDelimiter());

			
			// get corresponding subj/obj and create a table
			// off of it
			/*
			 * possible in hive:
			 * CREATE TABLE ext_table LOCATION '/user/XXXXX/XXXXXX' 
			 * AS SELECT * from managed_table;
			 * choose a different approach for improved compatibility
			 * => Do it via 2 statements instead of 1
			 */
			
			// drop VP
			runStaticSql(conn, getDropSql(tname), false);
			if(!isHive()){
				conn.commit();
			}
			
			// detect type
			final int otype = typeChecker.detectObjectType(pred);
			final String otypeStr = typeChecker.getTypeName(otype, isStringSupported());
			final boolean isTimestamp = (otype == Types.TIMESTAMP);
			// max. uri length
			String stypeStr = "VARCHAR(" + 1024 + ")";
			if (isStringSupported()) {
				stypeStr = "STRING";
			}
			
			// create VP
			runStaticSql(conn, getCreateVPSql(tname, stypeStr, otypeStr), false);
			if(!isHive()){
				conn.commit();
			}
			
			// get data which should be inserted
			filterStmt.setString(1, pred);
			ResultSet filtered = filterStmt.executeQuery();

			// insert into table
			String insSql = getInsertSql(tname, 2);
			PreparedStatement insertVP = prepareStatement(conn, insSql); 
			
			// if preparedStatement is not supported
						String bigInsert = "INSERT INTO " + tname + " VALUES ";
						
						boolean firstEntry = true;
						int vpSize = 0;
						while (filtered.next()) {
							// filterStmt.setObject(pos, val type_AS_INT)
							String sub = filtered.getString(1);
							String obj = filtered.getString(2);
							
							obj = Helper.cleanObject(obj, isTimestamp);
							
							if (isPrepareSupported()) {
								insertVP.setString(1, sub);
								insertVP.setObject(2, obj, otype);
								// there is also a length parameter which might be useful for decimal/numerical
								// but these values do not exist here

								// insert the data
								insertVP.executeUpdate();
							} else {
								// prepared statement is not supported
								// build 1 big INSERT sql statement
								String val = "('" + sub + "', ";
								if (typeChecker.quotationRequired(otype)) {
									val += "'" + obj + "')";
								} else {
									val = val + obj + ")";
								}
								if (firstEntry) {
									firstEntry = false;
									bigInsert += val;
								} else {
									bigInsert += ", " + val;
								}
								
							}
							vpSize++;
						}
						
						if (!isPrepareSupported()) {
							Statement bigInsertStmt = conn.createStatement();
							bigInsertStmt.executeUpdate(bigInsert);
							close(bigInsertStmt);
						}
			
			// close insertVP prepared statement
			close(insertVP);
			
			// add statistics
			stats.incSavedTables();
			stats.addVPStatistic(pred, vpSize);
			
			close(filtered);
		}
		
	}
	
	
	class ExtVPJob extends Job {

		private final String pred1;
		
		private final String relType;
		
		public ExtVPJob(String predicate1) {
			this.pred1 = predicate1;
			this.relType = reltype;
		}
		
		@Override
		public void runJob(Connection conn) throws Exception {
			String pred1Table = Helper.getPartName(pred1, getDelimiter());
			
			// get related predicates
			Statement relPredStmt = conn.createStatement();
			ResultSet relPred = relPredStmt.executeQuery(getLeftJoinSql(pred1Table, relType));

			// for each related predicate
			while(relPred.next()) {
				//String pred2 = Helper.getPartName(relPred.getString(1));
				String pred2 = relPred.getString(1);
				String pred2Table = Helper.getPartName(pred2, getDelimiter());

				int extVpTableSize = -1;

				// Don't create unnecessary tables
				if (!(relType == RELTYPE_SS && pred1.equals(pred2))) {
					String extVPSql = getExtVpSQLcommand(pred1Table, pred2Table, relType);

					// calculate size
					extVpTableSize = runStaticSql(conn, extVPSql, true).size() - 1;

					


					if (extVpTableSize < (stats.getVPTableSize(pred1) * scaleUB) ) {
						
						// - omit directory check -

						// save the extVP table
						String tableName =  relType + DELIM 
								+ pred1Table + DELIM + pred2Table;

						//Drop table
						runStaticSql(conn, getDropSql(tableName), false);
						
						// create & insert
						if (isPrepareSupported()) {
							Statement extVPStmt = conn.createStatement();
							ResultSet extVPRes = extVPStmt.executeQuery(extVPSql);

							resultSetToTable(conn, extVPRes, tableName);

							close(extVPRes);

							close(extVPStmt);
						} else {
							// create table as select ...
							sqlToTable(conn, tableName, extVPSql);
						}
						
						
						stats.incSavedTables();

					} else {
						stats.incUnsavedNonEmptyTables();
					}

					
				} else {
					extVpTableSize = stats.getVPTableSize(pred1Table);
				}
				
				
				// write statistics
				stats.addExtVPStatistic(pred1, pred2, extVpTableSize);
				
			}


			close(relPred);

			close (relPredStmt);
		}
		
		private void sqlToTable(Connection conn, String tableName, String dataSql) throws SQLException {
			String createSql = "CREATE TABLE " + tableName + " AS " + dataSql;
			
			runStaticSql(conn, createSql, false);
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
			    if (rsmd.getColumnTypeName(i).equalsIgnoreCase("VARCHAR"))
					if ( precision != 0 ) {
				    	createSql += "(" + precision + ")";
				    }
			}
			
			createSql += " )";
			
			// finally, create the table
			runStaticSql(conn, createSql, false);
			if(!isHive()){
				conn.commit();
			}
			
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
				//insertStmt.executeUpdate();
				insertStmt.addBatch();
			}
			insertStmt.executeUpdate();
			
			insertStmt.close();
		}
		
		
		
	}
	
	
	
	class JobExecuter extends SQLWrapper implements Runnable  {

		private Connection conn = null;
		
		private boolean doJobs = true;
		
		private PreparedStatement filterStmt = null;
		
		private boolean idle = true;
		
		public JobExecuter(Config conf) {
			
			super(conf);
			
			this.doJobs = true;
			this.idle = true;
			
			try {
				this.conn = init();
			} catch (Exception e) {
				// dont do anything if the connection could not be created
				this.doJobs = false;
				e.printStackTrace();
			}
		}
		
		/*
		private void init() throws SQLException {

			final String host = conf.getHost();
			final String db = conf.getDbName();
			final String dbuser = conf.getUser();
			final String dbpw = conf.getPw();
			
			System.out.println("Connection with values host=" + host + ", db=" + db + ", user=" + dbuser + ", pw=" + dbpw);
			
			String connectionUrl = conf.getDriver().getJDBCUri(host, db);
			
			try {
				// jdbc:hive2://localhost:10000/default", "hive", ""
				//conn = DriverManager.getConnection("jdbc:hive2://" + host + ":" + PORT + "/" + db, dbuser, dbpw);
				this.conn = DriverManager.getConnection(connectionUrl, dbuser, dbpw);
			} catch (SQLException sqle) {
				sqle.printStackTrace(System.out);
			}
		}
		
		*/
		
		
		public void resume() {
			this.idle = false;
		}
		
		/**
		 * Stop gracefully by setting flags
		 */
		public void stop() {
			this.doJobs = false;
			this.idle = false;
		}
		
		public boolean isIdle() {
			return this.idle;
		}

		@Override
		public void run() {
			while (doJobs) {
				if (!idle) {
					Job j = jobQueue.poll();

					if (j != null) {
						//System.out.println("Thread " + this.toString() + " running job!");
						// check if it is a VP job
						if (mode == MODE_VP) {
							// initialize filter statement if it wasn't initialized yet!
							// prepare filter SQL statements
							if (filterStmt == null) {
								String filterSql = getPredicateFilterSql();
								try {
									filterStmt = conn.prepareStatement(filterSql);
								} catch (SQLException e) {
									e.printStackTrace();
								}
							}
							// assign filter statement
							((VPJob) j).setFilterStmt(filterStmt);
						}

						// try to run the job
						try {
							j.runJob(conn);
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						// check if there might be new elements coming!
						if (producerFinished) {
							// System.out.println("Thread " + this.toString() + " stopping"); 
							// commit all the inserts
							try {
								if(!isHive()){
									conn.commit();
								}
							} catch (SQLException e) {
								e.printStackTrace();
							}
							idle = true;
							onThreadIdle();
						}

					}
				}
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			// when all the work is done: close the connection
			close(conn);
		}


	}

	
	
}
