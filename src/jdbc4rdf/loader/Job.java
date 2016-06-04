package jdbc4rdf.loader;

import java.sql.Connection;


public interface Job {

	public abstract void runJob(Connection conn) throws Exception ;
	
	
}
