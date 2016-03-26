package jdbc4rdf;

import java.util.logging.Logger;
import java.util.logging.Level;

import jdbc4rdf.core.config.DBDRIVER;
import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.loader.SQLDataLoader;
import jdbc4rdf.loader.impl.HiveDataLoader;

public class Main {


	private static Logger logger =  Logger.getLogger(String.valueOf(Main.class));
	
	
	
	public static void showHelp() {
		logger.info("> Loder Syntax:"); 
		logger.info("load DRIVER FILE HOST [DB] USER PW");
		logger.info("Supported drivers:");
		logger.info(DBDRIVER.getDriverList() + "\n");
		logger.info("> Executor Syntax:");
		logger.info("exec TODO...TODO");
	}
	
	
	public static void main(String[] args) {
		// TODO: parse arguments!
		
		
		if (args.length > 0) {
			if (args[0].equals("load")) {
				// loadConfig
				LoaderConfig loadconf;
				DBDRIVER driver;
				String file = "";
				String host = "";
				String db = "";
				String user = "";
				String pw = "";

				if (args.length == 7) {
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
				} else {
					logger.log(Level.SEVERE, "Not enough arguments given");
					showHelp();
					return;
				}
				
				// DBDRIVER driver, String file, String user, String pw, String host, String db
				loadconf = new LoaderConfig(driver, file, user, pw, host, db);
				
				// load data
				SQLDataLoader sql = null; // = new SQLDataLoader(loadconf);
				if (driver.equals(DBDRIVER.HIVE)) {
					sql = new HiveDataLoader(loadconf);
				} else if (driver.equals(DBDRIVER.MYSQL)) {
					// TODO
					// sql = new MySQLDataLoader(loadconf);
				}
				// run sql
				sql.runSql();
				
			} else if (args[0].equals("exec")) {
				// TODO: execConfig
				logger.log(Level.INFO, "Not yet implemented!");
			}
		} else {
			logger.log(Level.SEVERE, "Not enough arguments given");
			showHelp();
		}
		
		
	}

}
