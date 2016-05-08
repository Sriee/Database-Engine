package edu.utdallas.fileindex;

import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableSchemaManager {

	public static TreeMap<String,TreeMap<Integer,List<String>>> tableMap = new TreeMap<String,TreeMap<Integer,List<String>>>();
	public static TreeMap<String,TreeMap<String,TreeMap<Integer,List<String>>>> tableSchemaMap = new TreeMap<String,TreeMap<String,TreeMap<Integer,List<String>>>>();
	public static TreeMap<Integer,List<String>> ordinalMap = new TreeMap<Integer,List<String>>();

	//Making Table Schema Manager as a Singleton Class 
	private static TableSchemaManager _tableSchemaManager;

	private TableSchemaManager(){ }

	public static TableSchemaManager getTableSchemaManagerInstance(){
		if ( _tableSchemaManager == null)
			return new TableSchemaManager();
		return _tableSchemaManager;
	}

	/**
	 * Creates new table schema and the schema structure is stored still the end of 
	 * program
	 * 
	 * <schema_name<table_name<ordinal<column_name,data_type,data_type_size,nullable,primary_key>>>>
	 * 
	 * @param tableName	
	 * @param ordinalPosition
	 * @param columnName
	 * @param dataType
	 * @param isNullable
	 * @param isPrimaryKey
	 */
	public void newTableSchema(
			String tableName,
			int ordinalPosition,
			String columnName,
			String dataType,
			boolean isNullable,
			boolean isPrimaryKey){
		String val = "";

		List<String> column = new ArrayList<String>();
		column.add(columnName);

		String changedDataType = "";
		if(dataType.contains("(")){
			changedDataType = dataType.substring(0,dataType.indexOf("("));
			column.add(changedDataType.toUpperCase());
		}else{
			column.add(dataType.toUpperCase());
		}

		column.add(getSize(dataType));

		if(isNullable) val = "YES";
		else val = "NO";
		column.add(val);

		if(isPrimaryKey) val = "YES";
		else val = "NO";
		column.add(val);

		ordinalMap.put(ordinalPosition+1, column);

	}


	/**
	 * Calculates the size for the data type
	 * 
	 * @param type data type of a column
	 * @return size mapped size for the data type 
	 */
	private String getSize(String type){
		String size = "";
		switch(type.toUpperCase()){
		case "BYTE":
			size = "1";
			break;
		case "SHORT":
			size = "2";
			break;
		case "INT":
			size = "4";
			break;
		case "LONG":
			size = "8";
			break;
		case "FLOAT":
			size = "4";
			break;
		case "DOUBLE":
			size = "8";
			break;
		case "DATETIME":
			size = "8";
			break;
		case "DATE":
			size = "8";
			break;
		default:
			size = type.substring(type.indexOf("(")+1,type.indexOf(")"));
			break;
		}
		return size;
	}



	/**
	 * Checks the local table schema if the table exits in the current shcema
	 * and calculates the degree for the table
	 * 
	 * @param schemaName Current schema name
	 * @param tableName Table name to insert values
	 * @return degree Degree of the table
	 */
	public int getTableDegree(String schemaName, String tableName){
		int degree = 0;
		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaMap.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaIterator = tableSchemaSet.iterator();

		while(tableSchemaIterator.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>> me =  tableSchemaIterator.next();
			String schema = me.getKey();

			if(schema.equals(schemaName)){
				TreeMap<String,TreeMap<Integer,List<String>>> currentTable = me.getValue();
				Set<Map.Entry<String,TreeMap<Integer,List<String>>>> tableSet = currentTable.entrySet();
				Iterator<Map.Entry<String,TreeMap<Integer,List<String>>>> tableIterator = tableSet.iterator();

				while(tableIterator.hasNext()){
					Map.Entry<String, TreeMap<Integer,List<String>>> tableME = tableIterator.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						TreeMap<Integer,List<String>> ordinal = tableME.getValue();
						Set<Map.Entry<Integer,List<String>>> ordinalSet = ordinal.entrySet();
						Iterator<Map.Entry<Integer,List<String>>> ordinalIterator = ordinalSet.iterator();

						while(ordinalIterator.hasNext()){
							Map.Entry<Integer,List<String>> columnME = ordinalIterator.next();
							degree = columnME.getKey();
						}

					}
				}
			}
		}
		return degree;
	}

	/**
	 * Searches through existing schema map structures updated while program started. Looks for the column
	 * schema that mathced current schema name and table name
	 *    
	 * @param schemaName Name of the current schema 
	 * @param tableName Name of the current table 
	 * @return ordinal
	 */
	public TreeMap<Integer,List<String>> getColumnSchema(String schemaName, String tableName){
		TreeMap<Integer,List<String>> ordinal = new TreeMap<Integer,List<String>>();

		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaMap.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaIterator = tableSchemaSet.iterator();

		while(tableSchemaIterator.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>> me =  tableSchemaIterator.next();
			String schema = me.getKey();

			if(schema.equals(schemaName)){
				TreeMap<String,TreeMap<Integer,List<String>>> currentTable = me.getValue();
				Set<Map.Entry<String,TreeMap<Integer,List<String>>>> tableSet = currentTable.entrySet();
				Iterator<Map.Entry<String,TreeMap<Integer,List<String>>>> tableIterator = tableSet.iterator();

				while(tableIterator.hasNext()){
					Map.Entry<String, TreeMap<Integer,List<String>>> tableME = tableIterator.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						ordinal = tableME.getValue();
					}
				}
			}
		}
		return ordinal;
	}

	public void updateTableSchema(String schemaName, String tableName){
		TreeMap<String,TreeMap<Integer,List<String>>> loadNew = new TreeMap<String,TreeMap<Integer,List<String>>>();	
		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaMap.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaIterator = tableSchemaSet.iterator();

		if(tableSchemaSet.isEmpty()){
			Set<Map.Entry<String,TreeMap<Integer,List<String>>>> loadSet = tableMap.entrySet();
			Iterator<Map.Entry<String,TreeMap<Integer,List<String>>>> loadIterator = loadSet.iterator();
			while (loadIterator.hasNext()){
				Map.Entry<String,TreeMap<Integer,List<String>>> me = loadIterator.next();
				loadNew.put(me.getKey(), me.getValue());
			}
			TableSchemaManager.tableSchemaMap.put(schemaName, loadNew);
		} else {
			while(tableSchemaIterator.hasNext()){
				Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>> me =  tableSchemaIterator.next();
				String schema = me.getKey();

				if(schema.equals(schemaName)){
					TreeMap<String,TreeMap<Integer,List<String>>> currentTable = me.getValue();
					Set<Map.Entry<String,TreeMap<Integer,List<String>>>> tableSet = currentTable.entrySet();
					Iterator<Map.Entry<String,TreeMap<Integer,List<String>>>> tableIterator = tableSet.iterator();

					while(tableIterator.hasNext()){
						Map.Entry<String, TreeMap<Integer,List<String>>> tableME = tableIterator.next();
						loadNew.put(tableME.getKey(), tableME.getValue());
					}
					Set<Map.Entry<String,TreeMap<Integer,List<String>>>> loadSet = tableMap.entrySet();
					Iterator<Map.Entry<String,TreeMap<Integer,List<String>>>> loadIterator = loadSet.iterator();
					while (loadIterator.hasNext()){
						Map.Entry<String,TreeMap<Integer,List<String>>> meI = loadIterator.next();
						loadNew.put(meI.getKey(), meI.getValue());
					}
					TableSchemaManager.tableSchemaMap.put(schemaName, loadNew);
				}
			}
		}
	}
}
