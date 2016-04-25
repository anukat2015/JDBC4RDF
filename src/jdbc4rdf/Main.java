package jdbc4rdf;

import java.util.logging.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import jdbc4rdf.core.config.Config;
import jdbc4rdf.core.config.DBDRIVER;
import jdbc4rdf.core.config.ExecutorConfig;
import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.executor.ParseFile;
import jdbc4rdf.executor.Query;
import jdbc4rdf.executor.ResultWriter;
import jdbc4rdf.executor.SQLExecutor;
import jdbc4rdf.executor.impl.HiveExecutor;
import jdbc4rdf.executor.impl.MySQLExecutor;
import jdbc4rdf.loader.DataLoader;
import jdbc4rdf.loader.SQLDataLoader;
import jdbc4rdf.loader.impl.HiveDataLoader;
import jdbc4rdf.loader.impl.MySQLDataLoader;

public class Main {


	private static Logger logger =  Logger.getLogger(String.valueOf(Main.class));
	
	
	
	public static void showHelp() {
		logger.info("> Loder Syntax:"); 
		logger.info("load DRIVER FILE HOST [DB] USER PW SCALEUB");
		logger.info("Supported drivers: ");
		logger.info(DBDRIVER.getDriverList() + "\n");
		logger.info("SCALEUB should be a value between 0.0 and 1.0");
		logger.info("> Executor Syntax:");
		logger.info("exec DRIVER FILE HOST [DB] USER PW");
		logger.info("Supported drivers: ");
		logger.info(DBDRIVER.getDriverList() + "\n");
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
					logger.log(Level.SEVERE, "Not enough arguments given");
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
				sql.runSql();
				
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
				}
				else if (args.length == 6) {
					driver = DBDRIVER.detectDriver(args[1]);
					file = args[2];
					host = args[3];
					db = "";
					user = args[4];
					pw = args[5];
				} else {
					logger.log(Level.SEVERE, "Not enough arguments given");
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
				}
				
				// run sql
				sql.runSql();				
				
			}
		} else {
			logger.log(Level.SEVERE, "Not enough arguments given");
			showHelp();
		}
		
		
	}

}
