package jdbc4rdf.core.config;


public class LoaderConfig extends Config {

	
	
	
	private String dataFile = "";
	
	
	private float scaleUB = 1.0f;
	
	
	
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
		
		this.scaleUB = 1.0f;
	}
	
	
	public LoaderConfig(DBDRIVER driver, String file
			, String user, String pw, String host, String db
			, float scaleUBIn) {
		
		super(driver, user, pw, host, db);
		
		this.dataFile = file;
		
		this.scaleUB = scaleUBIn;
	}
	
	
	public String getDatafile() {
		return this.dataFile;
	}
	
	
	
	public float getScaleUB() {
		return this.scaleUB;
	}


	
	
	
	
}
