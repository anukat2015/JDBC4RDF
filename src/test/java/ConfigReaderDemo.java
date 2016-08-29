package test.java;

import jdbc4rdf.core.config.ConfigReader;
import jdbc4rdf.core.config.ExecutorConfig;
import jdbc4rdf.core.config.LoaderConfig;

public class ConfigReaderDemo {

	public static void main(String[] args) {
		// simulates parsing of configuration parameters
		ConfigReader reader = new ConfigReader(args);
		
		if (args[0].equals("load")) {
			
			// load configuration
			reader.getConfig(LoaderConfig.class); 
			
		} else if (args[0].equals("exec")) {
			
			// load configuration
			reader.getConfig(ExecutorConfig.class);
			
		}
	}

}
