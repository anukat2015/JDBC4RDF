package jdbc4rdf.core.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import jdbc4rdf.executor.SQLExecutor;

public class ConfigReader {

	/**
	 * Stores the values in the configuration file as key, value pairs
	 */
	private final Map<String, String> settings = new HashMap<String, String>();
	
	
	private static final String DB__AUTH_USER = "db.auth.user";
	private static final String DB_AUTH_PW = "db.auth.pw";
	private static final String DB_NAME = "db.name";
	private static final String DB_HOST = "db.host";
	private static final String DB_DRIVER = "db.driver";
	
	/**
	 * Query file name/path
	 */
	private static final String EXECUTOR_QUERYFILE = "executor.queryfile";
	
	/**
	 * Optional additional JDBC URI suffix
	 */
	private static final String EXECUTOR_URISUFFIX = "executor.urisuffix";
	
	/**
	 * Query name pattern (.contains)
	 */
	private static final String EXECUTOR_QUERY_NAMEPATTERN = "executor.query.namepattern";
	
	/**
	 * Query limit / index 
	 * (<empty> = all, 1 = only first, 5 = first 5, -5 = last five, 4:8 query 4 - query 8)
	 */
	private static final String EXECUTOR_QUERY_IDX = "executor.query.idx";

	
	private static final String LOADER_DATAFILE = "loader.datafile";
	private static final String LOADER_SCALEUB = "loader.scaleub";
	
	private String fpath = "settings.txt";
	private String[] args = new String[0];
	
	final static Logger logger = Logger.getLogger(SQLExecutor.class);
	
	
	
	public ConfigReader(String[] args) {
		
		// set defaults
		settings.put(DB__AUTH_USER, "root");
		settings.put(DB_AUTH_PW, "");
		settings.put(DB_NAME, "");
		settings.put(DB_HOST, "localhost");
		settings.put(DB_DRIVER, "hive");
		
		
		settings.put(EXECUTOR_QUERYFILE, "queries.txt");
		settings.put(EXECUTOR_URISUFFIX, "");
		settings.put(EXECUTOR_QUERY_NAMEPATTERN, "");
		settings.put(EXECUTOR_QUERY_IDX, "");
		
		
		settings.put(LOADER_DATAFILE, "data.tsv");
		settings.put(LOADER_SCALEUB, "1.0");
		
		// store arguments
		this.args = args;
		
		// set configuration file name
		this.fpath = args[1];
	}
	



	public <T extends Config> T getConfig(Class<T> type) {
		
		// load configuration from file
		loadFile();
		
		// check for additional modifiers in arguments
		loadArguments();
		
		T conf = null;
		
		
		DBDRIVER dbdriver = DBDRIVER.detectDriver(settings.get(DB_DRIVER).toString());
		
		if (type.equals(ExecutorConfig.class)) {
			conf = type.cast(new ExecutorConfig(dbdriver
					, settings.get(EXECUTOR_QUERYFILE)
					, settings.get(EXECUTOR_URISUFFIX)
					, settings.get(EXECUTOR_QUERY_NAMEPATTERN)
					, settings.get(EXECUTOR_QUERY_IDX)
					, settings.get(DB__AUTH_USER)
					, settings.get(DB_AUTH_PW)
					, settings.get(DB_HOST)
					, settings.get(DB_NAME)));
			
		} else if (type.equals(LoaderConfig.class)) {
			float scaleUB = 1.0f;
			String scaleUBStr = settings.get(LOADER_SCALEUB);
			try {
				scaleUB = Float.parseFloat(scaleUBStr);
			} catch (NumberFormatException nfe) {
				logger.error("Unable to parse float from value \"" + scaleUBStr + "\" Using default value(1.0)", nfe);
			}
			conf = type.cast(new LoaderConfig(dbdriver
					
					, settings.get(LOADER_DATAFILE)
					
					, settings.get(DB__AUTH_USER)
					, settings.get(DB_AUTH_PW)
					, settings.get(DB_HOST)
					, settings.get(DB_NAME)
					
					, scaleUB));
		} else {
			logger.error("Unable to load config for class " + type.getName());
		}
		
		return conf;
	}
	
	
	
	private void extractProperty(String line) {
		// key value pairs are separated by "="
		int idx = line.indexOf("=");
		String key = line.substring(0, idx).toLowerCase();
		String value = line.substring(idx + 1, line.length());
		settings.put(key, value);
		logger.debug("Found config key=\"" + key + "\", value=\"" + value + "\"");
	}
	
	
	/**
	 * Reads a file. For each line, this function does the
	 * following: <br>
	 * - Trim the line (as String) <br>
	 * - If the line is not empty: <br> 
	 * 	insert into internal data structure
	 * @throws IOException
	 */
	private void readFile() throws IOException {
		
		BufferedReader bf = null;
		FileInputStream fs = null;
		
		String line = "";
		
		try {
			fs = new FileInputStream(fpath);
			bf = new BufferedReader(new InputStreamReader(fs));
			
			while ((line = bf.readLine()) != null) {
				line = line.trim();
				
				// for each line
				// ignore empty lines
				// allow comments starting with #
				if (!(line.isEmpty() || line.startsWith("#") )) {
					extractProperty(line);
				}
				
				
			}
			
		} catch (IOException ioe) {
			logger.error("An error occured while trying to read file " + fpath, ioe);
		} finally {
			if (fs != null) fs.close();
			if (bf != null) bf.close();
		}
	}
	
	
	
	/**
	 * Calls the readFile function. 
	 * If the given file name does not exist, then the default
	 * settings will be loaded.
	 */
	private void loadFile() {

		// only read the file if it exists!
		if ( (new File(fpath)).exists() ) {
			
			// actions before reading - none -
			
			try {
				readFile();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
			
			// actions after reading - none -
		
		} else {
			logger.error("File \"" + fpath + "\" does not exist!");
		}
	}

	
	
	
	private void loadArguments() {
		// ARGS : exec|load filename PARAMETERS
		// PARAMETERS : key=value [key=value ...]
		for (int i = 2; i < args.length; i++) {
			String prop = args[i];
			if (prop.contains("=")) {
				extractProperty(prop);
			}
		}
	}

	
	
	
}
