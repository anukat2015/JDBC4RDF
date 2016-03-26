package jdbc4rdf.loader;

import java.sql.Connection;

import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.core.sql.SQLWrapper;


public class SQLDataLoader extends SQLWrapper {

	public SQLDataLoader(LoaderConfig loaderConf) {
		super(loaderConf);
	}

	
	
	
	@Override
	protected void loadData(Connection conn) throws Exception {
		// stmt exec
		// also work with prepareStatement method to improve performance
	}
	
	
	

}
