package jdbc4rdf;


import java.io.IOException;

import org.apache.log4j.Logger;

import jdbc4rdf.core.Helper;
import jdbc4rdf.core.config.ConfigReader;
import jdbc4rdf.core.config.DBDRIVER;
import jdbc4rdf.core.config.ExecutorConfig;
import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.executor.SQLExecutor;
import jdbc4rdf.executor.impl.HiveExecutor;
import jdbc4rdf.executor.impl.MySQLExecutor;
import jdbc4rdf.loader.DataLoader;
import jdbc4rdf.loader.impl.HiveDataLoader;
import jdbc4rdf.loader.impl.MySQLDataLoader;

public class Main {


	final static Logger logger = Logger.getLogger(Main.class);
	
	
	public static long startMilli = 0;
	
	
	public static void showHelp() {
		System.out.println("\nSyntax:"); 
		System.out.println("load|exec settingsFile [key=value[ key=value[ ...]]]");
		System.out.println("Key/value pairs can be used for overwriting the settings inside the .properties file");
		System.out.println("Supported drivers: ");
		System.out.println(DBDRIVER.getDriverList() + "\n");
	}	
	
	
	
	/*
	 * Sample commands
	 * # java -DLOG_DIR=/local/log/dir/ -jar jdbc4rdf_0.2.jar exec jdbc4rdf_vagrant.properties executor.queryfile=different/path/of/queries.txt
	 * # $(get_java_home)/bin/java -jar $(get_local_apps_path)/jdbc4rdf_0.2.jar exec $(get_local_apps_path)/jdbc4rdf_vagrant.properties executor.queryfile=$(get_local_apps_path)/queries.txt
	 */
	
	public static void main(String[] args) throws IOException {
		// Parse all the arguments
		Main.startMilli = System.currentTimeMillis();
		String timestamp = Helper.getTimestamp(Main.startMilli);
		
		logger.debug("Application start: " + timestamp);
		System.out.println("Application start: " + timestamp);
		
		
		
		DBDRIVER driver;
		
		if (args.length > 1) {
			
			ConfigReader reader = new ConfigReader(args);
			
			if (args[0].equals("load")) {
				
				// load configuration
				LoaderConfig loaderconf = reader.getConfig(LoaderConfig.class); 
				driver = loaderconf.getDriver();
				
				// Initialize loader
				DataLoader sql = null;
				if (driver.equals(DBDRIVER.HIVE)) {
					sql = new HiveDataLoader(loaderconf);
				} else if (driver.equals(DBDRIVER.MYSQL)) {
					sql = new MySQLDataLoader(loaderconf);
				}
				// Load data
				sql.loadData();
				
			} else if (args[0].equals("exec")) {
				
				// load configuration
				ExecutorConfig execconf = reader.getConfig(ExecutorConfig.class);
				driver = execconf.getDriver();
				
				// Initialize executor
				SQLExecutor sql = null;
				if (driver.equals(DBDRIVER.HIVE)) {
					sql = new HiveExecutor(execconf);
				} else if (driver.equals(DBDRIVER.MYSQL)) {
					sql = new MySQLExecutor(execconf);
				} else if (driver.equals(DBDRIVER.SPARK)) {
					sql = new HiveExecutor(execconf);
				}
				
				// Execute queries
				sql.executeQueries();				
				
			}
		} else {
			logger.fatal("Not enough arguments given");
			showHelp();
		}
		
		
	}

}
