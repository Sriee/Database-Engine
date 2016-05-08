package edu.utdallas.fileindex;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class InsertHelper {

	private static InsertHelper _insertHelper;

	private InsertHelper(){}

	public static InsertHelper getInsertHelperInstance(){
		if(_insertHelper == null)
			return new InsertHelper();
		return _insertHelper;
	}

	private TreeMap<Integer,List<String>> rowMap = new TreeMap<Integer,List<String>>();;
	private TreeMap<String,TreeMap<Integer,List<String>>> tableDataMap = new TreeMap<String,TreeMap<Integer,List<String>>>();
	public static TreeMap<String,TreeMap<String,TreeMap<Integer,List<String>>>> tableSchemaDataMap = new TreeMap<String,TreeMap<String,TreeMap<Integer,List<String>>>>();
	private static TreeMap<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>> tableIndexDataMap = new TreeMap<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>();
	private static int rowCount = 0;

	/**
	 * 
	 * @param schemaName Name of the current schema 
	 * @param tableName Name of the current table 
	 * @param columnValues The column values for the current table 
	 * @param columnSchema The schema's for individual column in the current table 
	 * @return updateSuccess True if the insert operation were successfull 
	 * 						 False if not
	 */
	public boolean insertValues(
			String schemaName,
			String tableName,
			String[] columnValues,
			TreeMap<Integer,List<String>> columnSchema){

		boolean updateSuccess = true;
		Set<Map.Entry<Integer,List<String>>> columnSet = columnSchema.entrySet();
		Iterator<Map.Entry<Integer,List<String>>> columnIterator = columnSet.iterator();
		List<String> columnData = new ArrayList<String>();

		while(columnIterator.hasNext()){

			Map.Entry<Integer,List<String>> columnME = columnIterator.next();
			int position = columnME.getKey();
			List<String> currentColumn = columnME.getValue();
			try {
			//Primary Key Checking 
			if(currentColumn.get(4).equals("YES")){
				if(checkPKConstraint(schemaName,tableName,position,columnValues[position - 1])){
					System.out.println("Primary Key Constraint Violation....");
					updateSuccess = false;
					break;
				} else if (columnValues[position - 1].equalsIgnoreCase("NULL") || columnValues[position - 1] == ""){
					System.out.println("Primary Key Can't be Null....");
					updateSuccess = false;
					break;
				} else if(currentColumn.get(1).equals("DATETIME")){ //Changing DATE TIME Format
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else if(currentColumn.get(1).equals("DATE")){ //Changing DATE Format
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else {
					columnData.add(columnValues[position - 1]);
				}
			} else if(currentColumn.get(3).equals("YES")){ //Checking for NULL Constraint violation
				if(columnValues[position - 1].equalsIgnoreCase("NULL") || columnValues[position - 1] == ""){
					System.out.println("Violates NULL Constraint...");
					updateSuccess = false;
					break;
				} else if(currentColumn.get(1).equals("DATETIME")){ //Changing DATE TIME Format
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else if(currentColumn.get(1).equals("DATE")){ //Changing DATE Format
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else {
					columnData.add(columnValues[position - 1]);
				}
			} else {
				if(currentColumn.get(1).equals("DATETIME")){ //Changing DATE TIME Format
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else if(currentColumn.get(1).equals("DATE")){ //Changing DATE Format
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else {
					columnData.add(columnValues[position - 1]);
				}
			}	
					
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		if(updateSuccess){
			rowCount = getRowCount(schemaName,tableName);

			InsertHelper.setRowCount(rowCount + 1);

			rowMap = updateRowMap(schemaName,tableName);
			rowMap.put(rowCount, columnData);

			tableDataMap.put(tableName, rowMap);

			InsertHelper.tableSchemaDataMap.put(schemaName,tableDataMap);
		}
		return updateSuccess;
	}

	/**
	 * 
	 * @param schemaName Name of the current schema
	 * @param tableName Current table name
	 * @return updatedRowMap update row map 
	 */
	public TreeMap<Integer,List<String>> updateRowMap(String schemaName,String tableName){
		TreeMap<Integer,List<String>> updatedRowMap = new TreeMap<Integer,List<String>>();
		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaDataMap.entrySet();
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
						updatedRowMap = tableME.getValue();	
					}
				}
			}
		}		
		return updatedRowMap;
	} // End of updateTableDataMap

	/**
	 * Checks whether the current insert statement has violated any primary key constraint 
	 * 
	 * @param schemaName Name of the current schema 
	 * @param tableName Current table name
	 * @param position position of the primary key in the list of columns
	 * @param col The column value which is to be tested with previous values
	 * @return True if primary key constraint is violated 
	 * @return False if not
	 */
	public boolean checkPKConstraint(String schemaName,String tableName,int position,String col){
		boolean isConstraintViolated = false;
		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaDataMap.entrySet();
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
							List<String> row = columnME.getValue();
							String rowValue = row.get(position - 1);
							if(rowValue.equals(col)){
								isConstraintViolated = true;
								break;
							}
						}

					}
				}
			}
		}
		return isConstraintViolated;
	}//End of checkPKConstraint

	/**
	 * Calculates the number of rows in the table. This count is used to update Information Schema 
	 * Row attribut 
	 * 
	 * @param schemaName Current schema name
	 * @param tableName Current tabe name 
	 * @return rowCount Number of the rows in the table
	 */
	public int getRowCount(String schemaName,String tableName){
		int rowCount = 0;
		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaDataMap.entrySet();
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
							rowCount = columnME.getKey();
						}

					}
				}
			}
		}
		return rowCount;
	} //End of getRowCount

	/**
	 * Sends the table data tree map to the calling function
	 * 
	 * @param schemaName Current schema name
	 * @param tableName Current tabe name
	 * @return tableDataMap
	 */
	public static TreeMap<Integer,List<String>> getTableData(String schemaName,String tableName){
		TreeMap<Integer,List<String>> tableDataMap = new TreeMap<Integer,List<String>>();

		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaDataMap.entrySet();
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
						tableDataMap = tableME.getValue();
					}
				}
			}
		}

		return tableDataMap;
	} //End of getTableData

	/**
	 * Sends the table index tree map to the calling function
	 * 
	 * @param schemaName Current schema name
	 * @param tableName Current tabe name
	 * @return tableIndexMap
	 */
	public static TreeMap<String,TreeMap<String,List<String>>> getIndexData(String schemaName,String tableName){
		TreeMap<String,TreeMap<String,List<String>>> tableIndexMap = new TreeMap<String,TreeMap<String,List<String>>>();

		Set<Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>> tableIndexDataSet = tableIndexDataMap.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>> tableIndexIterator = tableIndexDataSet.iterator();

		//Schema Comparison
		while(tableIndexIterator.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>> me = tableIndexIterator.next();
			String currentSchemaName = me.getKey();
			if(currentSchemaName.equals(schemaName)){
				TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>> tableIndex = me.getValue();
				Set<Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>>> tableIndexSet = tableIndex.entrySet();
				Iterator<Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>>> tableIterator = tableIndexSet.iterator();

				//Table Comparison
				while(tableIterator.hasNext()){
					Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>> tableME = tableIterator.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						tableIndexMap = tableME.getValue();
					}
				}
			}
		}
		return tableIndexMap;
	} //End of getIndexData

	/**
	 * Looks up the existing table index data tree map and updates any new entry
	 * to the index data map 
	 * 
	 * @param schemaName Name of the current Schema
	 * @param tableName Name of the current Table 
	 * @param columnName Name of the current column in insertion 
	 * @param pointerValue   
	 * @param key
	 */
	public void updateIndex(String schemaName,String tableName,String columnName,long pointerValue, String type,String key){
		TreeMap<String,TreeMap<String,List<String>>> tableIndexStream = new TreeMap<String,TreeMap<String,List<String>>>();

		List<String> indexValue = new ArrayList<String>();
		int pointerCounter = 0;

		TreeMap<String,List<String>> idx = new TreeMap<String,List<String>>();
		TreeMap<String,TreeMap<String,List<String>>> columnIndexMap = new TreeMap<String,TreeMap<String,List<String>>>();
		TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>> tableIndexMap = new TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>();  

		tableIndexStream = getCurrentIndexStream(schemaName,tableName,columnName);

		if(!tableIndexStream.isEmpty()){
			Set<Map.Entry<String,TreeMap<String, List<String>>>> tableIndexStreamSet = tableIndexStream.entrySet();
			Iterator<Map.Entry<String,TreeMap<String, List<String>>>> tableIndexStreamIterator = tableIndexStreamSet.iterator();

			while(tableIndexStreamIterator.hasNext()){
				Map.Entry<String,TreeMap<String,List<String>>> indexStreamME = tableIndexStreamIterator.next();
				String existingColumn = indexStreamME.getKey();
				TreeMap<String,List<String>> indexStreamValue = indexStreamME.getValue();

				Set<Map.Entry<String, List<String>>> indexStreamSet = indexStreamValue.entrySet();
				Iterator<Map.Entry<String, List<String>>> indexStreamIterator = indexStreamSet.iterator();

				while(indexStreamIterator.hasNext()){
					Map.Entry<String, List<String>> currentME = indexStreamIterator.next();
					String currentK = currentME.getKey();
					List<String> currentV = currentME.getValue();

					//Loading previous columns
					columnIndexMap.put(existingColumn,indexStreamValue);

					if(!currentV.isEmpty()){
						if(currentK.equals(key)){
							indexValue.add(currentV.get(0));
							//Increment pointer count 
							int currentPointerCounter = Integer.parseInt(currentV.get(1));
							pointerCounter = currentPointerCounter + 1;
							indexValue.add(Integer.toString(pointerCounter));

							for(int i = 0; i < currentPointerCounter; i++)
								indexValue.add(currentV.get(i + 2));

							indexValue.add(Long.toString(pointerValue));
						} else {
							pointerCounter = pointerCounter + 1;
							indexValue.add(type);
							indexValue.add(Integer.toString(pointerCounter));
							indexValue.add(Long.toString(pointerValue));
						}
					} else {
						pointerCounter = pointerCounter + 1;
						indexValue.add(type);
						indexValue.add(Integer.toString(pointerCounter));
						indexValue.add(Long.toString(pointerValue));
					}
				}
			}
		} else {
			pointerCounter = pointerCounter + 1;
			indexValue.add(type);
			indexValue.add(Integer.toString(pointerCounter));
			indexValue.add(Long.toString(pointerValue));
		}

		idx.put(key, indexValue);
		columnIndexMap.put(columnName,idx);
		tableIndexMap.put(tableName,columnIndexMap);
		InsertHelper.tableIndexDataMap.put(schemaName, tableIndexMap);
	}

	/**
	 * Looks at the existing tableIndexDataMap and returns the column stream of the table under the 
	 * current schema name
	 *  
	 * @param schemaName Name of the current schema
	 * @param tableName Name of the table where the index list has to be extracted
	 * @param columnName 
	 * @return indexStream index list for a particular column
	 */
	public TreeMap<String,TreeMap<String,List<String>>> getCurrentIndexStream(String schemaName,String tableName,String columnName){
		TreeMap<String,TreeMap<String,List<String>>> indexStream = new TreeMap<String,TreeMap<String,List<String>>>();

		Set<Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>> tableIndexDataSet = tableIndexDataMap.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>> tableIndexIterator = tableIndexDataSet.iterator();

		//Schema Comparison
		while(tableIndexIterator.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>> me = tableIndexIterator.next();
			String currentSchemaName = me.getKey();
			if(currentSchemaName.equals(schemaName)){
				TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>> tableIndex = me.getValue();
				Set<Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>>> tableIndexSet = tableIndex.entrySet();
				Iterator<Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>>> tableIterator = tableIndexSet.iterator();

				//Table Comparison
				while(tableIterator.hasNext()){
					Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>> tableME = tableIterator.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						indexStream = tableME.getValue();
					}
				}
			}
		}
		return indexStream;
	}

	/**
	 * @return the rowCount
	 */
	public static int getRowCount() {
		return rowCount;
	}

	/**
	 * @param rowCount the rowCount to set
	 */
	public static void setRowCount(int rowCount) {
		InsertHelper.rowCount = rowCount;
	}
}
