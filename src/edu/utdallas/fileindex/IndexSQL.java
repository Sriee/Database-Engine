package edu.utdallas.fileindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.Scanner;

/**
 * Entry class for the DavisBase Management System.
 * 
 * Handles the following queries
 * 		1. SHOW_SCHEMA
 *		2. USE_SCHEMA
 *		3. SHOW_TABLES
 *		4. CREATE_SCHEMA
 *		5. CREATE_TABLE
 *		6. INSERT
 *		7. SELECT_FROM_IS
 *		8. SELECT_FROM_IS_NOT
 *		9. SELECT_FROM_WHERE
 * 		10.SELECT_FROM
 *					
 * @author sriee
 *
 */
public class IndexSQL {
	static BufferedReader queryReader = new BufferedReader(new InputStreamReader(System.in));
	static final String IS_SCHEMATA_FILE = "information_schema.schemata.tbl";
	static final String IS_TABLE_FILE = "information_schema.table.tbl";
	static final String IS_COLUMN_FILE = "information_schema.columns.tbl";

	public static void main(String[] args) {

		String currentDirectory = System.getProperty("user.dir");
		File workingDirectory = new File(currentDirectory);
		boolean startDevisBase = false;
		boolean createTableFailed = false;
		boolean insertTableFailed = false;
		Schema schemaHandler = Schema.getSchemaInstance();
		TableHandler tableHandler = new TableHandler();
		try {
			//Checking whether schemata file is present or not 
			FilenameFilter schemataFilter = new FilenameFilter(){
				public boolean accept(File workingDirectory, String fileName){
					return fileName.equals(IS_SCHEMATA_FILE);
				}
			};
			String[] informationSchemaSchemata = workingDirectory.list(schemataFilter);

			//Checking whether information table file is present or not 
			FilenameFilter informationSchemaTableFilter = new FilenameFilter(){
				public boolean accept(File workingDirectory, String fileName){
					return fileName.equals(IS_TABLE_FILE);
				}
			};
			String[] informationSchemaTable = workingDirectory.list(informationSchemaTableFilter);

			//Checking whether information column file is present or not 
			FilenameFilter informationSchemaColumnFilter = new FilenameFilter(){
				public boolean accept(File workingDirectory, String fileName){
					return fileName.equals(IS_COLUMN_FILE);
				}
			};
			String[] informationSchemaColumn = workingDirectory.list(informationSchemaColumnFilter);

			if((informationSchemaSchemata.length == 1) && (informationSchemaTable.length == 1) && (informationSchemaColumn.length == 1)){
				System.out.println("Information Schema Found");
				startDevisBase = true;
			} else if(schemaHandler.initInformationSchema()){
				System.out.println("Initialized...");
				startDevisBase = true;
			} else
				startDevisBase = false;
		}catch (Exception e) {
			e.printStackTrace();
		}

		if(startDevisBase){
			splashScreen();

			//Declaring variables to be used 
			String query = null;
			String orgQuery = null;
			boolean flag = true;
			int selecttype = 0;
			
			String sqlPrompt = "devisql> ";
			StringBuilder builder = null;
			Scanner sc = new Scanner(System.in);
			SQLParser parseSQLQuery = null;
			parseSQLQuery = new SQLParser();

			try{
				while(true){
					String inputLine = "";
					builder = new StringBuilder();

					//Gets multi line query inputs from the user
					while(flag){
						System.out.print(sqlPrompt);	
						inputLine = sc.nextLine() + " ";
						builder.append(inputLine);
						if(inputLine.contains(";"))
							flag = false;
					}
					flag = true;
					
					orgQuery = builder.toString().trim().replace(";","");
					orgQuery = orgQuery.replace("'", "").trim();
					
					query = builder.toString().toLowerCase().trim().replace(";","");
					query = query.replace("'", "").trim();
					
					//Parse the inputed query
					parseSQLQuery.parse(query);

					switch(parseSQLQuery.getCurrentQueryType()){
					case ERROR: 
						System.out.println("Oops!! I couldn't understand your SQL syntax!!!");
						break;
					case SHOW_SCHEMA:
						schemaHandler.showSchema();
						break;
					case USE_SCHEMA:
						schemaHandler.useSchema(orgQuery);
						break;
					case SHOW_TABLES:
						tableHandler.showTables();
						break;
					case CREATE_SCHEMA:
						schemaHandler.createSchema(orgQuery);
						break;
					case CREATE_TABLE:
						createTableFailed = tableHandler.createTable(orgQuery);
						if(!createTableFailed) System.out.println("Query Ok!..");
						break;
					case INSERT:
						insertTableFailed = tableHandler.insertTable(orgQuery);
						if(!insertTableFailed) System.out.println("Query Ok!... 1 Row inserted...");
						break;
					case SELECT_FROM:
						selecttype = 1;
						tableHandler.selectTable(selecttype, orgQuery);
						break;
					case SELECT_FROM_WHERE:
						selecttype = 2;
						tableHandler.selectTable(selecttype, orgQuery);
						break;
					case SELECT_FROM_IS:
						selecttype = 3;
						tableHandler.selectTable(selecttype, orgQuery);
						break;
					case SELECT_FROM_IS_NOT:
						selecttype = 4;
						tableHandler.selectTable(selecttype, orgQuery);
						break;
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				sc.close();
			}
		} else {
			System.out.println("Oops!! Problem with information schema... Try Again!.... ");
		}
	}
	
	/**
	 * Display's the welcome screen after loading the values from information schema 
	 */
	public static void splashScreen() { 
		System.out.println();
		System.out.println(line("*",100));
		System.out.println();
		System.out.println("\t\t\t\tWelcome to DavisBase");
		System.out.println();
		System.out.println("\tSHOW SCHEMAS\t\t\t:  Displays all the schemas in the Database.");
		System.out.println("\tUSE [SCHEMA_NAME] \t\t:  Selects [SCHEMA_NAME] from the list of schemas.");
		System.out.println("\tSHOW TABLES\t\t\t:  Displays all the tables in the selected schema.");
		System.out.println("\tCREATE [SCHEMA_NAME]\t\t:  Creates [SCHEMA_NAME] in the Database.");
		System.out.println("\tCREATE TABLE [TABLE_NAME]\t:  Creates [TABLE_NAME] in the current schema.");
		System.out.println("\tINSERT INTO [TABLE_NAME]\t:  Inserts values into table.");
		System.out.println("\tSELECT * FROM WHERE\t\t:  Select all values from the table.");
		System.out.println("\tEXIT\t\t\t\t:  Exit the program.");
		System.out.println();
		System.out.println("\tPlease Note:");
		System.out.println("\t\t1. Application is Case Sensitive");
		System.out.println("\t\t2. Supports multi-line inputs");
		System.out.println("\t\t3. Uses ';' as Delimiter");
		System.out.println("\t\t4. All table and index files will be created in the current working directory");
		System.out.println();
		System.out.println(line("*",100));
	}
	
	/**
	 * Prints a line of *'s
	 * 
	 * @param s		
	 * @param num
	 * @return
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
}
