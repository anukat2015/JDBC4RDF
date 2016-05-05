package jdbc4rdf.executor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import jdbc4rdf.core.config.Config;
import jdbc4rdf.core.config.ExecutorConfig;
import jdbc4rdf.core.sql.SQLWrapper;

public class SQLExecutor extends SQLWrapper implements Executor {

	private String compositeQueries = "";
	
	public SQLExecutor(Config confIn) {
		super(confIn);
		
		this.compositeQueries = ((ExecutorConfig) confIn).getCompositeFile();
	}
	
	@Override
	public void executeQueries() {
		try {
			queriesRun(conn);
			
		} catch (SQLException | IOException e) {
			e.printStackTrace();
			rollback(conn);
			
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
		List<Query> queries = ParseFile.getQueries(compositeQueries);
		ArrayList<String> result = new ArrayList<String>();
		ResultWriter resWriter = new ResultWriter();
		
		//Result Header
		String header = "Query Name; Result(s); Runtime (in ms)";
		result.add(header);
		
		//Execute every query in list
		for(int i = 0; i < queries.size(); i++){
			Statement stmt = conn.createStatement();
			
			System.out.println("Executing query " + Integer.toString(i) + "...");
			long startTime = System.currentTimeMillis();
			// run query
			ResultSet res = runQuery(stmt, queries.get(i).query);
			long endTime = System.currentTimeMillis();
			System.out.println("Done!");
			
			// calculate runtime
			long executionTime = endTime - startTime;
			int results = 0;
			
			// count results
			if(!(res == null)){
				while(res.next()) {
				    results++;
				}
			}
			
			//adding line of results to ArrayList which will be wrote in to a file
			String footer = queries.get(i).queryName + "; " + Integer.toString(results) + "; " + Long.toString(executionTime);
			result.add(footer);
			
		}
		
		//create new File
		resWriter.newFile();
		// write results
		resWriter.writeFile(result);
		
	}



	
	
	

}
