package jdbc4rdf.executor;

public class Table {
	
	protected String tableName = "";
	protected String tableType = "";
	
	
	public Table(String tableName, String tableType) {
		super();
		this.tableName = tableName;
		this.tableType = tableType;
	}


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public String getTableType() {
		return tableType;
	}


	public void setTableType(String tableType) {
		this.tableType = tableType;
	}
	
	

}
