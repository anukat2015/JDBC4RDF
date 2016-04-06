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

public abstract class SQLExecutor extends SQLWrapper{

	private String compositeQueries = "";
	
	public SQLExecutor(Config confIn) {
		super(confIn);
		
		this.compositeQueries = ((ExecutorConfig) confIn).getCompositeFile();
	}
	
	public void runSql() {
		
		Connection conn = null;
		//Statement stmt = null;
		try {
			// init
			conn = init();
			
			conn.setAutoCommit(AUTOCOMMIT);
			
			// do import
			queriesRun(conn);
			
		} catch (Exception e) {
			e.printStackTrace();
			rollback(conn);
		} finally {
			close(conn);
		}
	}

	private void queriesRun(Connection conn) throws IOException, SQLException {
		
		List<Query> queries = ParseFile.getQueries(compositeQueries);
		ArrayList<String> result = new ArrayList<String>();
		ResultWriter resWriter = new ResultWriter();
		
		//Result Header
		String header = "Query Name; Result(s); Runtime";
		result.add(header);
		
		for(int i = 0; i < queries.size(); i++){
			Statement stmt = conn.createStatement();
			
			long startTime = System.currentTimeMillis();
			ResultSet res = runQuery(stmt, queries.get(i).query);
			long endTime = System.currentTimeMillis();
			
			long executionTime = endTime - startTime;
			int results = 0;
			
			if(!(res == null)){
				while(res.next()) {
				    results++;
				}
			}
			
			String footer = queries.get(i).queryName + "; " + Integer.toString(results) + "; " + Long.toString(executionTime);
			result.add(footer);
			
		}
		
		resWriter.newFile();
		resWriter.writeFile(result);
		
	}
	
	

}
