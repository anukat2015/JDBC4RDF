package jdbc4rdf.executor.impl;

import jdbc4rdf.core.config.ExecutorConfig;
import jdbc4rdf.executor.SQLExecutor;

/**
 * 
 * Some spark specific code could be implemented here.
 *
 */
public class SparkExecutor extends SQLExecutor{

	public SparkExecutor(ExecutorConfig confIn) {
		super(confIn);
	}

}