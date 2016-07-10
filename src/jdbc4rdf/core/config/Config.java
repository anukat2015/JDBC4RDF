package jdbc4rdf.core.config;

public class Config {


	protected DBDRIVER dbDriver = null;


	protected String dbUser = "root";

	protected String dbPw = "rootpw";

	protected String dbHost = "localhost";

	protected String dbName = "defaultdb";

	protected String uriSuffix = "";
	
	public Config(DBDRIVER driver, String user, String pw, String host, String db, String urisuffix) {
		this.dbDriver = driver;

		this.dbUser = user;
		this.dbPw = pw;

		this.dbHost = host;
		this.dbName = db;
		
		this.uriSuffix = urisuffix;
	}
	

	public String getUriSuffix() {
		return this.uriSuffix;
	}
	
	public DBDRIVER getDriver() {
		return this.dbDriver;
	}
	
	public String getUser() {
		return this.dbUser;
	}
	
	public String getHost() {
		return this.dbHost;
	}
	
	public String getPw() {
		return this.dbPw;
	}
	
	public String getDbName() {
		return this.dbName;
	}
	
	
}
