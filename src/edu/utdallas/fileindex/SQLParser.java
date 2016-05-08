package edu.utdallas.fileindex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class SQLParser {


	private String showSchemaExp = null;
	private String useExp = null;
	private String showTablesExp = null;
	private String createSchemaExp = null;
	private String createTableExp = null;
	private String selectExpSFWI = null;
	private String selectExpSFWN = null;
	private String selectExpSFW = null;
	private String selectExpSF = null;
	private String insertExp = null;
	private String exitExp = null;
	public QueryType currentQueryType;
	
	private Pattern selectSFWIPattern, selectSFWNPattern, selectSFWPattern, selectSFPattern, insertPattern,exitPattern;
	private Pattern	showSchemaPattern,usePattern,showTablePattern, createSchemaPattern,createTablePattern;

	/** Assigns integer value to current query type 
	 */
	public enum QueryType{
		ERROR,
		SHOW_SCHEMA,
		USE_SCHEMA,
		SHOW_TABLES,
		CREATE_SCHEMA,
		CREATE_TABLE,
		INSERT,
		SELECT_FROM_IS,
		SELECT_FROM_IS_NOT,
		SELECT_FROM_WHERE,
		SELECT_FROM
	};
	
	/** Setter method to set the current query type 
	 * 
	 * @param current Variable which is used to set the current query type
	 */
	public void setCurrentQueryType(QueryType current){
		this.currentQueryType = current;
	}
	
	/** Getter method to get the current query type 
	 * 
	 * @return the current query type
	 *   
	 */
	public QueryType getCurrentQueryType(){
		return this.currentQueryType;
	}
	/** Constructor which initializes the strings for matching user input query with the pattern
	 * 
	 */
	public SQLParser(){

		this.showSchemaExp = "SHOW\\s+?SCHEMAS";
		this.useExp = "USE\\s+?[^\\s]+";
		this.showTablesExp = "SHOW\\s+TABLES";
		this.createSchemaExp = "CREATE\\s+SCHEMA\\s+[^\\s]+";
		this.createTableExp = "CREATE\\s+?TABLE\\s+?(\\w)+\\s*\\(\\s*?([\\w\\,\\s*\\.\\(\\)0-9]|[\\w\\s*\\.\\(\\)0-9])+?\\s*\\)"; 
		this.selectExpSFWI = "SELECT\\s+?[*]\\s+?FROM\\s+?[^\\s]+?\\s+?(WHERE.*\\s+?[^\\s]+?\\s+?IS\\s+?NULL+)";
		this.selectExpSFWN = "SELECT\\s+?[*]\\s+?FROM\\s+?[^\\s]+?\\s+?(WHERE.*\\s+?[^\\s]+?\\s+?IS\\s+?NOT\\s+?NULL)";
		this.selectExpSFW = "SELECT\\s+?[*]\\s+?FROM\\s+?[^\\s]+?\\s+?(WHERE.*\\s+?[^\\s]+?\\s*?[<|>|=|>=|<=]\\s*?[^\\s]+)";
		this.selectExpSF = "SELECT\\s+?[*]\\s+?FROM\\s+?[^\\s]+";
		this.insertExp = "INSERT\\s+?INTO\\s+?[^\\s]+?\\s+?VALUES\\s*?\\(\\s?([\\w\\,\\s*\\-\\'\\.]|[\\w])+?\\)";
		this.exitExp = "exit\\s*?";

		initializePattern();
	}

	/** Method to initialize the pattern to be used to match the input query
	 *
	 */
	private void initializePattern(){
		this.showSchemaPattern = Pattern.compile(this.showSchemaExp, Pattern.MULTILINE | Pattern.DOTALL |Pattern.CASE_INSENSITIVE);
		this.usePattern = Pattern.compile(this.useExp, Pattern.MULTILINE | Pattern.DOTALL |Pattern.CASE_INSENSITIVE);
		this.showTablePattern = Pattern.compile(this.showTablesExp,Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.createSchemaPattern = Pattern.compile(this.createSchemaExp,Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.createTablePattern = Pattern.compile(this.createTableExp,Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.selectSFWIPattern = Pattern.compile(this.selectExpSFWI, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.selectSFWNPattern = Pattern.compile(this.selectExpSFWN, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.selectSFWPattern = Pattern.compile(this.selectExpSFW, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.selectSFPattern = Pattern.compile(this.selectExpSF, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.insertPattern = Pattern.compile(this.insertExp, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.exitPattern = Pattern.compile(this.exitExp, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
	}

	/**
	 * Matches the input query with existing matching pattern and sets 
	 * the current query type from the list of supported query types 
	 * supported by DavisBase
	 * 
	 * @param query input query from the user
	 */
	public void parse(String query){
		Matcher matcher = this.selectSFWIPattern.matcher(query);
		
		try{
			if(this.exitPattern.matcher(query).matches()){
				System.out.println("Exiting DavisBase...Bye...");  //EXIT
				System.exit(0);
			}else if(this.showSchemaPattern.matcher(query).matches()){ 
				setCurrentQueryType(QueryType.SHOW_SCHEMA);  //SHOW SCHEMA	
			}else if(this.usePattern.matcher(query).matches()){
				setCurrentQueryType(QueryType.USE_SCHEMA);  //USE TABLE_NAME
			}else if(this.showTablePattern.matcher(query).matches()){ 
				setCurrentQueryType(QueryType.SHOW_TABLES); //SHOW TABLES	
			}else if(this.createSchemaPattern.matcher(query).matches()){ 
				setCurrentQueryType(QueryType.CREATE_SCHEMA); //CREATE SCHEMA SCHEMA_NAME	
			}else if(this.createTablePattern.matcher(query).matches()){ 
				setCurrentQueryType(QueryType.CREATE_TABLE); //CREATE TABLE
			}else if(matcher.matches()){
				setCurrentQueryType(QueryType.SELECT_FROM_IS); //SELECT-FROM-WHERE IS 
			}else if(this.selectSFWNPattern.matcher(query).matches()){
				setCurrentQueryType(QueryType.SELECT_FROM_IS_NOT); //SELECT-FROM-WHERE IS NOT
			}else if(this.selectSFWPattern.matcher(query).matches()){
				setCurrentQueryType(QueryType.SELECT_FROM_WHERE); //SELECT-FROM-WHERE
			}else if(this.selectSFPattern.matcher(query).matches()){
				setCurrentQueryType(QueryType.SELECT_FROM); //SELECT-FROM
			}else if(this.insertPattern.matcher(query).matches()){
				setCurrentQueryType(QueryType.INSERT); //INSERT
			}else{
				setCurrentQueryType(QueryType.ERROR); //ERROR
			}
		}catch(PatternSyntaxException pe){
			System.out.println("Error in Pattern Syntax!!!");
			pe.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
