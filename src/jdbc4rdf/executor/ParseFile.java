package jdbc4rdf.executor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;


public class ParseFile {
	
	/**
	 * Input file is read and list of queries will be generated. 
	 * @param fpath String - Path to composite query file
	 * @return list of queries
	 * @throws IOException
	 */
	public static List<Query> getQueries(String fpath) throws IOException{
		
		String fileInString = readFile(fpath);
		List<Query> queries = new ArrayList<Query>();
		
		//Split different queries in file by '>>>>>>'
		String[] divFile = fileInString.split(">>>>>>");
		
		for(int i = 0; i < divFile.length; i++){
			if(divFile[i].length() > 0){
				//split query and table statistic
				String[] parts = divFile[i].split("\\+\\+\\+\\+\\+\\+");
				String sqlQuery = parts[0];
				//query name is mentioned after '>>>>>>' and ends with '\n'
				String queryName = sqlQuery.substring(0, sqlQuery.indexOf("\n"));
				sqlQuery = sqlQuery.substring(sqlQuery.indexOf("\n")+1);
				// Extract renaming of tables in FROM part for Spark 
				sqlQuery = sqlQuery.replaceAll("(?=\\$\\$)(.*)(?<=\\$\\$)", "");
				String qStats = parts[1].substring(parts[1].indexOf("\n")+1);
				//every table statistic is seperarted by '------\n'
				String[] tableStats = qStats.split("------\n");
				HashMap<String, Table> tables = new HashMap<String, Table>();
				
				for(int j = 0; j < tableStats.length; j++){
					if(tableStats[j].length() > 0){
						String[] bestTable = tableStats[j].substring(0, tableStats[j].indexOf("\n")).split("\t");
						String tableName = bestTable[0];
						String bestId = bestTable[1];
						String tableType = bestTable[2];
						String tablePath = bestTable[3];
						
						//initiate table with the given information of the statistic (path and id isn't needed)
						Table table = new Table(tableName, tableType);
						tables.put(tableName, table);	
					}
				}
				Query resQuery = new Query(queryName, sqlQuery, qStats, tables);
				queries.add(resQuery);				
			}
		}
		
		return queries;
	}
	
	/**
	 * Get's a input path of a file and returns a string of the entire file.
	 * @param fpath String path to file
	 * @return whole file in one String
	 * @throws IOException
	 */
	private static String readFile(String fpath) throws IOException{
		
		BufferedReader bf = null;
		FileInputStream fs = null;
		
		String line = "";
		String file = "";
		
		try {
			fs = new FileInputStream(fpath);
			bf = new BufferedReader(new InputStreamReader(fs));

			file = FileUtils.readFileToString(new File(fpath));
			/*
			while ((line = bf.readLine()) != null) {
				if (!line.isEmpty()){
					file = file.concat(line);
					if(!line.equals("")){
						file.concat("\n");
					}
				}
			}
			*/
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (fs != null) fs.close();
			if (bf != null) bf.close();
		}
		
		return file;
		
	}

}
