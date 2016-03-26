package jdbc4rdf.core.config;

import java.util.logging.Logger;

public enum DBDRIVER {
	MYSQL, HIVE;


	private static Logger logger =  Logger.getLogger(String.valueOf(DBDRIVER.class));
	
	public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	
	
	public static DBDRIVER getDefaultDriver() {
		return MYSQL;
	}
	
	
	public static DBDRIVER detectDriver(String dstr) {
		if ((dstr.equalsIgnoreCase("mysql")) ||
				dstr.equalsIgnoreCase(MYSQL_DRIVER)) {
			return MYSQL;
		} else if ((dstr.equalsIgnoreCase("mysql")) ||
				dstr.equalsIgnoreCase(HIVE_DRIVER)) {
			return HIVE;
		} else {
			logger.warning("Couldn't detect driver " + dstr);
			return null;
		}
		
	}
	
	public String getDriverClass() {
		return DBDRIVER.getDriverClass(this);
	}
	
	public static String getDriverClass(DBDRIVER d) {
		if (d.equals(MYSQL)) {
			return MYSQL_DRIVER; 
		} else if (d.equals(HIVE)) {
			return HIVE_DRIVER;
		} else {
			logger.warning("Unknown driver " + d.toString() + " - returning null");
			return null;
		}
	}
	
	public static String getDriverList() {
		return "mysql, hive";
	}
}
