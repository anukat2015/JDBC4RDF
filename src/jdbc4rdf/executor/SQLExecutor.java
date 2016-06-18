package jdbc4rdf.executor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import jdbc4rdf.Main;
import jdbc4rdf.core.Helper;
import jdbc4rdf.core.config.ExecutorConfig;
import jdbc4rdf.core.sql.SQLWrapper;

public class SQLExecutor extends SQLWrapper<ExecutorConfig> implements Executor {

	//private String compositeQueries = "";
	
	final static Logger logger = Logger.getLogger(SQLExecutor.class);
	
	public SQLExecutor(ExecutorConfig confIn) {
		super(confIn);
		// this.compositeQueries = ((ExecutorConfig) confIn).getCompositeFile();
	}
	
	
	
	
	public static int altmod(int a, int mbase) {
		int x = a % mbase;
		
		// In Java, negative a returns negative output, this has to be changed
		if (x < 0) {
			x += mbase;
		}
		
		return x;
	}

	//  <empty> = all, 1 = only first, 5 = first 5, -5 = last five, 4:8 query 4 - query 8
	// FILTER WORKS JUST LIKE THE PYTHON ARRAY SYNTAX


	public static <T> List<T> applyFilter(List<T> queries, String filter) {

		if (filter.isEmpty()) {
			return queries;
		}

		logger.info("Filtering " + queries.size() + " queries with filter \"" + filter + "\"");
		// loop constraints
		int start = 0;
		int stop = queries.size();
		int step = 1;


		// check if there is a :
		if (filter.contains(":")) {

			// start:end is the syntax
			int idx = filter.indexOf(":");
			String[] tokens = new String[]{filter.substring(0, idx), filter.substring(idx+1)};

			logger.debug("Found " + tokens.length + " tokens");
			logger.debug(Arrays.toString(tokens));


			if (!tokens[0].isEmpty()) {
				int val = Integer.parseInt(tokens[0]);
				start = altmod((start + val), stop);
			}
			if (!tokens[1].isEmpty()) {
				int val = Integer.parseInt(tokens[1]);
				stop = altmod((stop + val), stop);
			}

		} else {
			int val = Integer.parseInt(filter);
			/*
			 * sum = 10
			 * 3 -> 3
			 * 5 -> 5
			 * -3 > 7
			 */
			start = altmod((start + val), stop);
			stop = start+1;
		}



		// make sure the sign is correct
		if (start != stop) step = step * Integer.signum(stop - start);

		logger.debug("start=" + start);
		logger.debug("stop=" + stop);
		logger.debug("step=" + step);

		List<T> output = new ArrayList<T>();
		for(int i = start; i < stop; i += step) {
			output.add(queries.get(i));
		}
		return output;
	}


	private <T extends Query> List<T> applyNamePattern(List<T> queries, String np) {

		if (np.isEmpty()) {
			return queries;
		}

		List<T> output = new ArrayList<T>();
		for (int i = 0; i < queries.size(); i++) {
			T query = queries.get(i);
			if (query.queryName.contains(np)) {
				output.add(query);
			}
		}

		return output;
	}



	@Override
	public void executeQueries() {
		try {
			
			queriesRun(conn);
			
		} catch (SQLException | IOException e) {
			logger.error("A problem occured while trying to execute the queries", e);
			//rollback(conn);
		} finally {
			close(conn);
		}
		
	}
	

	/**
	 * Run the queries. Read the file and parse to a list of queries.
	 * Write the results in a file.
	 * @param Connection conn
	 * @throws IOException
	 * @throws SQLException
	 */
	private void queriesRun(Connection conn) throws IOException, SQLException {
		
		//parse file in List of queries
		List<Query> queries = ParseFile.getQueries(conf.getCompositeFile());
		
		
		// Filter the queries
		// ... by name
		queries = applyNamePattern(queries, conf.getNamePattern());
		// ... by index
		queries = applyFilter(queries, conf.getIdxStr());
		
		
		ArrayList<String> result = new ArrayList<String>();
		ResultWriter resWriter = new ResultWriter();
		
		// Result Header
		String header = "Query Name; Result(s); Runtime (in ms)";
		result.add(header);
		
		// Execute every query in list
		for(int i = 0; i < queries.size(); i++){
			Statement stmt = conn.createStatement();
			Query q = queries.get(i);
			
			System.out.println("Executing query " + Integer.toString(i+1) + "...");
			
			logger.info("Executing query " + Integer.toString(i+1) + "...");
			logger.info(q.queryName);
			logger.debug(q.query + "\n");
			
			long startTime = System.currentTimeMillis();
			// run query
			ResultSet res = runQuery(stmt, q.query);
			long endTime = System.currentTimeMillis();
			
			// Print query start timestamp
			System.out.println("Query start timestamp: "
					+ Helper.getTimestamp(startTime) );
			logger.info("Query start timestamp: "
					+ Helper.getTimestamp(startTime) );
			
			// make sure the milliseconds format gets converted to nanoseconds
			System.out.println("Time passed between application start and query execution:");
			logger.info("Time passed between application start and query execution:");
			Helper.printTime((startTime - Main.startMilli) * 1_000_000);
			
			// Calculate runtime
			long executionTime = endTime - startTime;
			
			System.out.println("Done in " + executionTime + "ms");
			logger.info("Done in " + executionTime + "ms");
			
			Helper.printTime(executionTime * 1_000_000);
			
			int results = 0;
			
			// Count results
			if(!(res == null)){
				while(res.next()) {
				    results++;
				}
			}
			
			System.out.println("Query returned " + results + " results \n\n");
			logger.info("Query returned " + results + " results \n\n");
			
			// Adding line of results to ArrayList which will be wrote in to a file
			String footer = q.queryName + "; " + Integer.toString(results) + "; " + Long.toString(executionTime);
			result.add(footer);
			
		}
		
		//create new File
		resWriter.newFile();
		// write results
		resWriter.writeFile(result);
		
	}



	
	
	

}
