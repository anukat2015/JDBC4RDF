package jdbc4rdf.executor;

public class Table {
	
	protected String tableName = "";
	protected String tableType = "";
	
	/**
	 * 
	 * Constructor to generate a table. Consist only of Name and Type
	 * @param tableName String
	 * @param tableType String
	 */
	public Table(String tableName, String tableType) {
		super();
		this.tableName = tableName;
		this.tableType = tableType;
	}	

}
