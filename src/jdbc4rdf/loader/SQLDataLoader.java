package jdbc4rdf.loader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import jdbc4rdf.core.config.LoaderConfig;
import jdbc4rdf.core.sql.SQLWrapper;


public abstract class SQLDataLoader extends SQLWrapper {

	private String dataFile = "";
	
	private final String TT_NAME = "triples";
	
	public SQLDataLoader(LoaderConfig loaderConf) {
		super(loaderConf);
		
		this.dataFile = loaderConf.getDatafile();
	}

	
	
	
	@Override
	protected void loadData(Connection conn) throws Exception {
		// stmt exec
		// also work with prepareStatement method to improve performance
		createTripleTable(conn);
	}
	
	
	private int createTripleTable(Connection conn) throws SQLException {
		
		int rows = -1;
		
		// remove table first
		String dropSql = getDropSql();
		PreparedStatement dropTable = prepareStatement(conn, dropSql);
		dropTable.setString(1, TT_NAME);
		dropTable.executeUpdate();
		
		close(dropTable);
		
		
		// create table
		String ctSql = getCreateTTSql();
		PreparedStatement ctStmt = prepareStatement(conn, ctSql);
		ctStmt.setString(1, TT_NAME);
		ctStmt.executeUpdate();
		
		close(ctStmt);
		
		
		// load tsv
		String loadSql = getLoadSql();
		PreparedStatement st = prepareStatement(conn, loadSql);
		st.setString(1, dataFile);
		st.setString(2, TT_NAME);
		st.executeUpdate();
		
		close(st);
		
		
		// count entries
		String rcSql = getRowCountSql();
		PreparedStatement countTriples = prepareStatement(conn, rcSql);
		countTriples.setString(1, TT_NAME);
		ResultSet rs = countTriples.executeQuery();
		rows = rs.getInt(1);
		
		close(rs);
		close(countTriples);
		
		return rows;
	}
	
	
	private PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException {
		PreparedStatement dropStmt = conn.prepareStatement(sql);
		
		return dropStmt;
	}

	
	/**
	 * 
	 * @return A sql statement which drops a given table. The table should be
	 * marked with a ?
	 */
	protected abstract String getDropSql();
	
	/**
	 * 
	 * @return A sql statement which creates a tripleTable with
	 * the given name and the schema sub:string, pred:string, obj:string
	 */
	protected abstract String getCreateTTSql();
	
	/**
	 * 
	 * @return A sql statement which load a given (tab-separated) file 
	 * into a given table. Both should be marked by ? place holders
	 */
	protected abstract String getLoadSql();
	
	/**
	 * 
	 * @return  A sql statement which counts the rows of a given table
	 */
	protected abstract String getRowCountSql();
	
}
