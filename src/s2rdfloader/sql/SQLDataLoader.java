package s2rdfloader.sql;

import java.sql.Statement;

public class SQLDataLoader extends SQLWrapper {

	public SQLDataLoader(String inHost, String inUser, String inPw, String inDb) {
		super(inHost, inUser, inPw, inDb);
	}

	
	
	
	@Override
	protected void loadData(Statement stmt) throws Exception {
		// stmt exec
	}
	
	
	

}
