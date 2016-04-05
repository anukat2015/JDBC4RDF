package jdbc4rdf.core.config;

public class ExecutorConfig extends Config {

	private String compositeFile = "";
	
	public ExecutorConfig(DBDRIVER driver, String file, String user, String pw, String host,
			String db) {
		super(driver, user, pw, host, db);
		
		this.compositeFile = file;
	}

}
