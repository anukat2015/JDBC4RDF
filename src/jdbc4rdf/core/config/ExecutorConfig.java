package jdbc4rdf.core.config;

public class ExecutorConfig extends Config {

	private String compositeFile = "";
	
	private String namePattern = "";
	
	private String idxStr = "";
	
	
	public ExecutorConfig(DBDRIVER driver, String file, String uriSuffix, String namepattern, String idx, String user, String pw, String host,
			String db) {
		super(driver, user, pw, host, db, uriSuffix);
		
		this.compositeFile = file;
		this.namePattern = namepattern;
		this.idxStr = idx;
	}
	
	public String getCompositeFile() {
		return this.compositeFile;
	}
	
	public String getNamePattern() {
		return this.namePattern;
	}
	
	public String getIdxStr() {
		return this.idxStr;
	}
	
	

}
