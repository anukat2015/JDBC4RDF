package jdbc4rdf.executor.impl;

import jdbc4rdf.core.config.ExecutorConfig;
import jdbc4rdf.executor.SQLExecutor;

/**
 * 
 * Some hive specific code could be implemented here.
 *
 */
public class HiveExecutor extends SQLExecutor{

	public HiveExecutor(ExecutorConfig confIn) {
		super(confIn);
	}

}
