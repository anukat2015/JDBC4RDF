package jdbc4rdf.core.config;


public class LoaderConfig extends Config {

	
	
	
	private String dataFile = "";
	
	
	
	
	/**
	 * Initialize the config with default values <br>
	 * driverName = DBDRIVER.getDefaultDriver(); <br>
	 * file = "data.txt"
	 * dbUser = "root"; <br>
	 * dbPw = "rootpw"; <br>
	 * dbHost = "localhost"; <br>
	 * dbName = "defaultdb"; 
	 */
	public LoaderConfig() {
		
		//super(dbDriver, dbUser, dbPw, dbHost, dbName);
		
		super(DBDRIVER.getDefaultDriver(), "root", "rootpw", "localhost", "");
		
		
		

		this.dataFile = "data.txt";
	}
	
	
	public LoaderConfig(DBDRIVER driver, String file, String user, String pw, String host, String db) {
		
		super(driver, user, pw, host, db);
		
		this.dataFile = file;
	}
	
	
	public String getDatafile() {
		return this.dataFile;
	}
	


	
	
	
	
}
