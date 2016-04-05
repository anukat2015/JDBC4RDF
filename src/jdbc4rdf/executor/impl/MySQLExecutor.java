package jdbc4rdf.executor.impl;

import java.sql.Connection;

import jdbc4rdf.core.config.Config;
import jdbc4rdf.executor.SQLExecutor;

public class MySQLExecutor extends SQLExecutor{

	public MySQLExecutor(Config confIn) {
		super(confIn);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void loadData(Connection conn) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
