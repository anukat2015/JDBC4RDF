package jdbc4rdf.executor;

import jdbc4rdf.core.config.Config;
import jdbc4rdf.core.config.ExecutorConfig;
import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.core.sql.SQLWrapper;

public abstract class SQLExecutor extends SQLWrapper{

	private String compositeQueries = "";
	
	public SQLExecutor(Config confIn) {
		super(confIn);
		
		this.compositeQueries = ((ExecutorConfig) confIn).getCompositeFile();
	}

}
