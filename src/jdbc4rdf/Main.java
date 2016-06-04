package jdbc4rdf;


import java.io.IOException;

import org.apache.log4j.Logger;

import jdbc4rdf.core.config.Config;
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
	
	
	
	public static void showHelp() {
		System.out.println("> Loder Syntax:"); 
		System.out.println("load DRIVER FILE HOST [DB] USER PW SCALEUB");
		System.out.println("Supported drivers: ");
		System.out.println(DBDRIVER.getDriverList() + "\n");
		System.out.println("SCALEUB should be a value between 0.0 and 1.0");
		System.out.println("> Executor Syntax:");
		System.out.println("exec DRIVER FILE HOST [DB] USER PW");
		System.out.println("Supported drivers: ");
		System.out.println(DBDRIVER.getDriverList() + "\n");
	}	
	
	public static void main(String[] args) throws IOException {
		// Parse all the arguments
		
		Config conf;
		DBDRIVER driver;
		
		if (args.length > 0) {
			if (args[0].equals("load")) {
				
				// Initialize a loadConfig instance
				String file = "";
				String host = "";
				String db = "";
				String user = "";
				String pw = "";
				float scaleUb = 1;
				
				if (args.length == 8) {
					driver = DBDRIVER.detectDriver(args[1]);
					file = args[2];
					host = args[3];
					db = args[4];
					user = args[5];
					pw = args[6];
					scaleUb = Float.parseFloat(args[7]);
				} else if (args.length == 7) {
					driver = DBDRIVER.detectDriver(args[1]);
					file = args[2];
					host = args[3];
					db = "";
					user = args[4];
					pw = args[5];
					scaleUb = Float.parseFloat(args[6]);
				} else {
					logger.fatal("Not enough arguments given");
					showHelp();
					return;
				}
				
				// DBDRIVER driver, String file, String user, String pw, String host, String db
				conf = new LoaderConfig(driver, file, user, pw, host, db, scaleUb);
				
				// load data
				DataLoader sql = null;
				if (driver.equals(DBDRIVER.HIVE)) {
					sql = new HiveDataLoader(conf);
				} else if (driver.equals(DBDRIVER.MYSQL)) {
					sql = new MySQLDataLoader(conf);
				}
				// run sql
				sql.loadData();
				
			} else if (args[0].equals("exec")) {
				
				String file = "";
				String host = "";
				String db = "";
				String user = "";
				String pw = "";
				
				if(args.length == 7){
					driver = DBDRIVER.detectDriver(args[1]);
					file = args[2];
					host = args[3];
					db = args[4];
					user = args[5];
					pw = args[6];
				} else if (args.length == 6) {
					driver = DBDRIVER.detectDriver(args[1]);
					file = args[2];
					host = args[3];
					db = "";
					user = args[4];
					pw = args[5];
				} else if (args.length == 5) {
					driver = DBDRIVER.detectDriver(args[1]);
					file = args[2];
					host = args[3];
					db = "";
					user = args[4];
					pw = "";
				} else {
					logger.fatal("Not enough arguments given");
					showHelp();
					return;
				}
				// Create config
				conf = new ExecutorConfig(driver, file, user, pw, host, db);
				
				// Detect driver
				SQLExecutor sql = null;
				
				if (driver.equals(DBDRIVER.HIVE)) {
					sql = new HiveExecutor(conf);
				} else if (driver.equals(DBDRIVER.MYSQL)) {
					sql = new MySQLExecutor(conf);
				} else if (driver.equals(DBDRIVER.SPARK)) {
					sql = new HiveExecutor(conf);
				}
				
				// run sql
				sql.executeQueries();				
				
			}
		} else {
			logger.fatal("Not enough arguments given");
			showHelp();
		}
		
		
	}

}
