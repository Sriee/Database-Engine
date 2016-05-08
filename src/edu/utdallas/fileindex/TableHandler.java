package edu.utdallas.fileindex;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
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

public class TableHandler {

	private static String tableName = null;
	private static int noOfColumns = 0;

	/**
	 * Creates new table in the current schema
	 * Creates the schema_name_table.table.tbl
	 * Creates the schema_name_table.table.tbl.idx
	 *  
	 * @param query Create table query from the user
	 * @throws IOException 
	 * */
	public boolean createTable(String input) throws IOException{
		String orgQuery = input;
		orgQuery = orgQuery.trim();
		String query = input.toLowerCase().trim();
		boolean queryFailed = false;
		//Schema Instance 
		Schema currentSchema =  Schema.getSchemaInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();

		//TableSchemaManager Instance
		TableSchemaManager currentTableSchemaManager =  TableSchemaManager.getTableSchemaManagerInstance();

		//Extracting Table name
		tableName = orgQuery.substring(query.indexOf("table ")+6,query.indexOf("(")).trim();

		//Updating SCHEMA.TABLE.TBL
		if(updateInformationSchemaTable()){
			queryFailed = true;
			return queryFailed;
		}

		//Extracting Table contents
		String tableContentsWithQuotes = orgQuery.substring(orgQuery.indexOf("(")+1,orgQuery.length());
		String tableContents = tableContentsWithQuotes.replace("))", ")");
		tableContents = tableContents.trim();

		TableSchemaManager.ordinalMap = new TreeMap<Integer,List<String>>();
		TableSchemaManager.tableMap = new TreeMap<String,TreeMap<Integer,List<String>>>();
		
		//Creating n instances of Table helper
		String[] createTableContents = tableContents.split("\\,");  
		noOfColumns = createTableContents.length;
		CreateTableHelper[] th = new CreateTableHelper[noOfColumns];

		
		//Handles single row of Create Table 
		for(int item = 0; item < noOfColumns ; item++){
			th[item] = new CreateTableHelper(); 
			//To remove any leading or trailing spaces
			createTableContents[item] = createTableContents[item].trim();
			String columnName = createTableContents[item].substring(0, createTableContents[item].indexOf(" "));
			th[item].setColumnName(columnName);

			//Setting Primary Key
			String primaryKeyExp = "(.*)[pP][rR][iI][mM][aA][rR][yY](.*)";
			if (createTableContents[item].matches(primaryKeyExp))
				th[item].setPrimaryKey(true);
			else
				th[item].setPrimaryKey(false);

			//Setting Null Value	
			String notNullExp = "(.*)[nN][uU][lL][lL](.*)";
			if (createTableContents[item].matches(notNullExp))
				th[item].setNull(true);
			else
				th[item].setNull(false);

			//Extracting data types 
			//BYTE
			String byteExp = "(.*)[bB][yY][tT][eE](.*)";
			if (createTableContents[item].matches(byteExp)){
				th[item].setDataType("BYTE");
			}
			//SHORT
			String shortExp = "(.*)[sS][hH][oO][rR][tT](.*)";
			if (createTableContents[item].matches(shortExp)){
				th[item].setDataType("SHORT");
			}
			//INT
			String intExp = "(.*)[iI][nN][tT](.*)";
			if (createTableContents[item].matches(intExp)){
				th[item].setDataType("INT");
			}
			//LONG
			String longExp = "(.*)[lL][oO][nN][gG](.*)";
			if (createTableContents[item].matches(longExp)){
				th[item].setDataType("LONG");
			}
			//CHAR
			String charExp = "(.*)[cC][hH][aA][rR](.*)";
			if (createTableContents[item].matches(charExp)){
				String size = createTableContents[item].substring(createTableContents[item].indexOf("(")+1, createTableContents[item].indexOf(")"));
				th[item].setDataType("CHAR(" + size + ")");
			}
			//VARCHAR
			String varcharExp = "(.*)[vV][aA][rR][cC][hH][aA][rR](.*)";
			if (createTableContents[item].matches(varcharExp)){
				String size = createTableContents[item].substring(createTableContents[item].indexOf("(")+1, createTableContents[item].indexOf(")"));
				th[item].setDataType("VARCHAR(" + size + ")");
			}
			//FLOAT
			String floatExp = "(.*)[fF][lL][oO][aA][tT](.*)";
			if (createTableContents[item].matches(floatExp)){
				th[item].setDataType("FLOAT");				
			}
			//DOUBLE
			String doubleExp = "(.*)[dD][oO][uU][bB][lL][eE](.*)";
			if (createTableContents[item].matches(doubleExp)){
				th[item].setDataType("DOUBLE");
			}
			//DATETIME
			String dateTimeExp = "(.*)[dD][aA][tT][eE][tT][iI][mM][eE](.*)";
			if (createTableContents[item].matches(dateTimeExp)){
				th[item].setDataType("DATETIME");
			}
			//DATE
			String dateExp = "(.*)[dD][aA][tT][eE](.*)";
			if (createTableContents[item].matches(dateExp)){
				th[item].setDataType("DATE");
			}

			currentTableSchemaManager.newTableSchema(
					tableName,
					item,
					th[item].getColumnName(),
					th[item].getDataType(),
					th[item].isNull(),
					th[item].isPrimaryKey()
					);

			//Updating SCHEMA.COLUMNS.TBL
			updateInformationSchemaColumn(
					th[item].getColumnName(),
					item,
					th[item].getDataType(),
					th[item].isNull(),
					th[item].isPrimaryKey()
					);
			//Create tables to insert index
			String newTableIndexName = currentSchemaName + "." + tableName + "." +th[item].getColumnName()+ ".tbl.ndx";
			RandomAccessFile newTableIndexFile = new RandomAccessFile(newTableIndexName, "rw");
			newTableIndexFile.close();

		}

		TableSchemaManager.tableMap.put(tableName, TableSchemaManager.ordinalMap);
		currentTableSchemaManager.updateTableSchema(currentSchemaName,tableName);
		
		//Create tables to insert data 
		String newTableDataName = currentSchemaName + "." + tableName + ".tbl";
		RandomAccessFile newTableDataFile = new RandomAccessFile(newTableDataName, "rw");
		newTableDataFile.close();
		return queryFailed;
	} //End of createTable

	/**
	 * Adds schema and table name to the information_schema.table.tbl file 
	 */
	public boolean updateInformationSchemaTable(){
		Schema currentSchema =  Schema.getSchemaInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();
		boolean isFound = false;
		try {
			RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema.table.tbl", "rw");

			//Searching to see if the information schema is present or not
			while(tablesTableFile.getFilePointer() < tablesTableFile.length()){
				String readSchemaName = "";
				byte varcharLength = tablesTableFile.readByte();
				for(int j = 0; j < varcharLength; j++)
					readSchemaName += (char)tablesTableFile.readByte();

				if(readSchemaName.equals(currentSchemaName)){
					String readTableName = "";
					byte varcharTableLength = tablesTableFile.readByte();
					for(int j = 0; j < varcharTableLength; j++)
						readTableName += (char)tablesTableFile.readByte();

					if(readTableName.equals(tableName)){
						isFound = true;
						System.out.println("Table '" + tableName + "' already exits...");
						break;
					}

					tablesTableFile.readLong();
				} else {
					byte traverseLength = tablesTableFile.readByte();
					for(int j = 0; j < traverseLength; j++)
						tablesTableFile.readByte();
					tablesTableFile.readLong();
				}	
			}

			if(!isFound){
				//Traversing to the end of file
				tablesTableFile.seek(tablesTableFile.length());
				tablesTableFile.writeByte(currentSchemaName.length()); // TABLE_SCHEMA
				tablesTableFile.writeBytes(currentSchemaName);
				tablesTableFile.writeByte(tableName.length()); // TABLE_NAME
				tablesTableFile.writeBytes(tableName);
				tablesTableFile.writeLong(0); // TABLE_ROWS
			}

			tablesTableFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		return isFound;
	} //End of updateInformationSchemaTable

	/**
	 * Adds schema and table name to the information_schema.table.tbl file
	 * 
	 * @param columnName
	 * @param ordinalPosition
	 * @param columnType
	 * @param isNull
	 * @param isPrimaryKey
	 */
	public void updateInformationSchemaColumn(
			String columnName,
			int ordinalPosition,
			String columnType,
			boolean isNull,
			boolean isPrimaryKey
			){
		Schema currentSchema =  Schema.getSchemaInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();
		try {
			RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");

			//Seeking to the end of file	
			columnsTableFile.seek(columnsTableFile.length());
			columnsTableFile.writeByte(currentSchemaName.length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes(currentSchemaName);
			columnsTableFile.writeByte(tableName.length()); // TABLE_NAME
			columnsTableFile.writeBytes(tableName);
			columnsTableFile.writeByte(columnName.length()); // COLUMN_NAME
			columnsTableFile.writeBytes(columnName);
			columnsTableFile.writeInt(ordinalPosition+1); // ORDINAL_POSITION
			columnsTableFile.writeByte(columnType.length()); // COLUMN_TYPE
			columnsTableFile.writeBytes(columnType);

			if (isNull) {
				columnsTableFile.writeByte("YES".length()); // IS_NULLABLE
				columnsTableFile.writeBytes("YES");
			} else {
				columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
				columnsTableFile.writeBytes("NO");
			}

			if (isPrimaryKey) {
				columnsTableFile.writeByte("PRI".length()); // COLUMN_KEY
				columnsTableFile.writeBytes("PRI");
			} else {
				columnsTableFile.writeByte("".length()); // COLUMN_KEY
				columnsTableFile.writeBytes("");
			}

			//Closing the file
			columnsTableFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	} //End of updateInformationSchemaColumn


	/**
	 * Prints the list of tables present in the current schema to the user
	 */
	public void showTables(){
		Schema schemaTable =  Schema.getSchemaInstance();
		String currentSchemaName = schemaTable.getCurrentSchema();

		try{
			ArrayList<String> tableList = new ArrayList<String>();

			RandomAccessFile tableFile = new RandomAccessFile("information_schema.table.tbl","rw");

			while(tableFile.getFilePointer() < tableFile.length()){
				String readSchemaName = "";
				String readTableName = "";

				//Looks for matching schema name
				byte varcharLength = tableFile.readByte();
				for(int j = 0; j < varcharLength; j++)
					readSchemaName += (char)tableFile.readByte();

				byte varcharTableLength = tableFile.readByte();
				for(int k = 0; k < varcharTableLength; k++)
					readTableName += (char)tableFile.readByte();
				//Looks for matching table name
				if(readSchemaName.equals(currentSchemaName)){	
					tableList.add(readTableName);
				}
				//To skip the number of rows part
				tableFile.readLong();
			}

			if(tableList.size() != 0){
				//Printing current Tables in the schema
				System.out.println("------------------------------------------------");
				System.out.println("Table_in_" + currentSchemaName);
				System.out.println("------------------------------------------------");
				for(int i = 0; i < tableList.size() ; i++)
					System.out.println(tableList.get(i));
				System.out.println("------------------------------------------------");

				//Clearing table list contents
				tableList.removeAll(tableList);
			} else {
				System.out.println("Empty Set...");
			}

			//Closing the file
			tableFile.close();
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
	} //End of showTables

	public boolean isTablePresent(String currentSchemaName, String table){
		boolean isTablePresent = false;
		try{
			RandomAccessFile tableFile = new RandomAccessFile("information_schema.table.tbl","rw");

			while(tableFile.getFilePointer() < tableFile.length()){
				String readSchemaName = "";
				String readTableName = "";

				byte varcharLength = tableFile.readByte();
				for(int j = 0; j < varcharLength; j++)
					readSchemaName += (char)tableFile.readByte();

				byte varcharTableLength = tableFile.readByte();
				for(int k = 0; k < varcharTableLength; k++)
					readTableName += (char)tableFile.readByte();

				//Looks for matching table name
				if(readSchemaName.equals(currentSchemaName)){
					if(readTableName.equals(table)){
						isTablePresent = true;
						break;
					}
				}

				//To skip the number of rows part
				tableFile.readLong();
			}

			if(!isTablePresent)
				System.out.println(currentSchemaName + "." + table +" doesn't exist...");

			//Closing the file
			tableFile.close();
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
		return isTablePresent;
	} //End of isTablePresent

	/**
	 * Inserts the values to appropriate table. Stores the data inside internal 
	 * data structure till the program exists. 
	 *   
	 * @param input Insert query statement
	 */
	public boolean insertTable(String input){
		TableSchemaManager tsm = TableSchemaManager.getTableSchemaManagerInstance();
		boolean isInsertSuccessfull = false;
		boolean isInsertTableFailed = false;
		String orgQuery = input;
		orgQuery = orgQuery.trim();
		String query = input.toLowerCase().trim();

		tableName = orgQuery.substring(query.indexOf("into ")+5,query.indexOf(" values")).trim();

		String colNames = orgQuery.substring(query.indexOf("(")+1,query.indexOf(")"));
		String[] spacedColNames = colNames.split(",");
		String[] selectColNames = new String[spacedColNames.length];
		//Trimming spaces
		for (int m = 0; m < spacedColNames.length; m++)
			selectColNames[m] = spacedColNames[m].trim();

		Schema currentSchema =  Schema.getSchemaInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();

		if(isTablePresent(currentSchemaName,tableName)){

			if(selectColNames.length == tsm.getTableDegree(currentSchemaName, tableName)){
				InsertHelper insertHelper = InsertHelper.getInsertHelperInstance();
				isInsertSuccessfull = insertHelper.insertValues(currentSchemaName, tableName,selectColNames,tsm.getColumnSchema(currentSchemaName, tableName));
				if(isInsertSuccessfull){
					//Updating information_schema.table.tbl
					updateISSchemaRow(currentSchemaName,InsertHelper.getRowCount());

					//Updating schema_name.table_name.tbl
					updateTableData(currentSchemaName,
							InsertHelper.getTableData(currentSchemaName,tableName),
							tsm.getColumnSchema(currentSchemaName, tableName)
							);

					//Updating schema_name.table_name.column_name.tbl.ndx
					updateIndexTable(currentSchemaName,
							InsertHelper.getIndexData(currentSchemaName,tableName)
							);

				} else 
					isInsertTableFailed = true;
			} else {
				System.out.println("Create schema, table and try again!...");
				isInsertTableFailed = true;
			}
		}
		return isInsertTableFailed;
	} //End of insertTable

	/**
	 * Writes to the Index files for each column 
	 * 
	 * @param currentSchema Name of the current schema 
	 * @param indexMap k,v Map with values and the positions that is to be updated in each table.column.tbl.ndx 
	 */
	public void updateIndexTable(String currentSchema,
			TreeMap<String,TreeMap<String,List<String>>> indexMap
			){

		//Table Index Map
		Set<Map.Entry<String,TreeMap<String,List<String>>>> indexMapSet = indexMap.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,List<String>>>> indexIterator = indexMapSet.iterator();

		while(indexIterator.hasNext()){

			Map.Entry<String,TreeMap<String,List<String>>> columnME = indexIterator.next();
			String currentColumn = columnME.getKey();
			TreeMap<String,List<String>> currentColumnKV = columnME.getValue();

			Set<Map.Entry<String,List<String>>> currentColumnKVSet = currentColumnKV.entrySet();
			Iterator<Map.Entry<String,List<String>>> currentColumnKVIterator = currentColumnKVSet.iterator();

			while(currentColumnKVIterator.hasNext()){
				Map.Entry<String,List<String>> currentColumnKVME = currentColumnKVIterator.next();
				String columnK = currentColumnKVME.getKey();
				List<String> columnV = currentColumnKVME.getValue();
				String openIndexFileName = currentSchema + "." + tableName + "." + currentColumn + ".tbl.ndx";
				try {
					RandomAccessFile openedIndexFile = new RandomAccessFile(openIndexFileName, "rw");
					openedIndexFile.seek(openedIndexFile.length());

					String type = columnV.get(0);
					String pointerCount = columnV.get(1);

					if(type.equalsIgnoreCase("CHAR") || type.equalsIgnoreCase("VARCHAR")){
						openedIndexFile.writeByte(columnK.length());
						openedIndexFile.writeBytes(columnK);
						openedIndexFile.writeInt(Integer.parseInt(pointerCount));
						for(int i = 0;i < Integer.parseInt(pointerCount); i++)
							openedIndexFile.writeInt(Integer.parseInt(columnV.get(i + 2)));		
					} else {
						switch(type){
						case "BYTE": 
							openedIndexFile.writeByte(Integer.parseInt(columnK));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnV.get(i + 2)));
							break;
						case "SHORT":
							openedIndexFile.writeShort(Integer.parseInt(columnK));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnV.get(i + 2)));
							break;
						case "INT": 
							openedIndexFile.writeInt(Integer.parseInt(columnK));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnV.get(i + 2)));
							break;
						case "LONG": 
							openedIndexFile.writeLong(Integer.parseInt(columnK));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnV.get(i + 2)));
							break;
						case "FLOAT": 
							openedIndexFile.writeFloat(Integer.parseInt(columnK));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnV.get(i + 2)));
							break;
						case "DOUBLE": 
							openedIndexFile.writeDouble(Integer.parseInt(columnK));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnV.get(i + 2)));
							break;
						case "DATETIME": 	
							DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
							Date dateTime = dateTimeFormat.parse(columnK);
							openedIndexFile.writeLong(dateTime.getTime());
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnV.get(i + 2)));
							break;
						case "DATE": 	
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
							Date date = dateFormat.parse(columnK);
							openedIndexFile.writeLong(date.getTime());							
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnV.get(i + 2)));
							break;
						}
					}
					openedIndexFile.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	} //End of updateIndexTable

	/**
	 * Updates the row count in Information Schema Table file
	 * 
	 * @param currentSchemaName
	 * @param rowCount
	 */
	public void updateISSchemaRow(String currentSchemaName,int rowCount){
		try {
			RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema.table.tbl", "rw");
			tablesTableFile.seek(0);

			//Searching to see if the information schema is present or not
			while(tablesTableFile.getFilePointer() < tablesTableFile.length()){
				String readSchemaName = "";
				byte varcharLength = tablesTableFile.readByte();
				for(int j = 0; j < varcharLength; j++)
					readSchemaName += (char)tablesTableFile.readByte();

				if(readSchemaName.equals(currentSchemaName)){
					String readTableName = "";
					byte varcharTableLength = tablesTableFile.readByte();
					for(int j = 0; j < varcharTableLength; j++)
						readTableName += (char)tablesTableFile.readByte();

					if(readTableName.equals(tableName)){
						tablesTableFile.writeLong(rowCount); 
						break;
					} else {
						tablesTableFile.readLong();
					}
				} else {
					byte traverseLength = tablesTableFile.readByte();
					for(int j = 0; j < traverseLength; j++)
						tablesTableFile.readByte();
					tablesTableFile.readLong();
				}	
			}
			tablesTableFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	} //End of updateISSchemaRow

	/**
	 * Writes the table data to the table data file 
	 * 
	 * @param currentSchema Name of the current schema
	 * @param tableDataMap K,V pair of table data 
	 * @param tableSchemaMap k,V pair of table schema
	 */
	public void updateTableData(String currentSchema,
			TreeMap<Integer,List<String>> tableDataMap,
			TreeMap<Integer,List<String>> tableSchemaMap){
		try{
			InsertHelper iH = InsertHelper.getInsertHelperInstance();
			RandomAccessFile tableDataFile = new RandomAccessFile(currentSchema + "." + tableName + ".tbl", "rw");

			//Table Data Map
			Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMap.entrySet();
			Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

			//Table Schema Map
			Set<Map.Entry<Integer,List<String>>> columnSet = tableSchemaMap.entrySet();
			Iterator<Map.Entry<Integer,List<String>>> columnIterator = columnSet.iterator();
			List<String> columnSchema = new ArrayList<String>();

			//Adding Column_Name,INT serially in the list
			while(columnIterator.hasNext()){
				Map.Entry<Integer,List<String>> columnME = columnIterator.next();
				List<String> currentColumn = columnME.getValue();
				columnSchema.add(currentColumn.get(0));
				columnSchema.add(currentColumn.get(1));
			}

			//Checks the array list and writes accordingly to the File 
			while(tableDataMapIterator.hasNext()){
				Map.Entry<Integer,List<String>> columnME = tableDataMapIterator.next();

				List<String> currentColumn = columnME.getValue();
				int columnDataCounter = currentColumn.size();
				int columnSchemaCounter = 0;

				for(int i = 0;i < columnDataCounter; i++){
					long tableIndexPointer = tableDataFile.getFilePointer();
					if(columnSchema.get(columnSchemaCounter + 1).contains("VARCHAR")){
						tableDataFile.writeByte(currentColumn.get(i).length());
						tableDataFile.writeBytes(currentColumn.get(i));
						iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
						columnSchemaCounter = columnSchemaCounter + 2;
					} else {
						switch(columnSchema.get(columnSchemaCounter + 1)){
						case "CHAR":
							tableDataFile.writeByte(currentColumn.get(i).length());
							tableDataFile.writeBytes(currentColumn.get(i));
							iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
							columnSchemaCounter = columnSchemaCounter + 2;
							break;
						case "BYTE": 
							tableDataFile.writeBytes(currentColumn.get(i));
							iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
							columnSchemaCounter = columnSchemaCounter + 2;
							break;
						case "SHORT":
							tableDataFile.writeShort(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
							columnSchemaCounter = columnSchemaCounter + 2;
							break;
						case "INT":
							tableDataFile.writeInt(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
							columnSchemaCounter = columnSchemaCounter + 2;
							break;
						case "LONG": 
							tableDataFile.writeLong(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
							columnSchemaCounter = columnSchemaCounter + 2;
							break;
						case "FLOAT": 
							tableDataFile.writeFloat(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
							columnSchemaCounter = columnSchemaCounter + 2;
							break;
						case "DOUBLE": 
							tableDataFile.writeDouble(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
							columnSchemaCounter = columnSchemaCounter + 2;
							break;
						case "DATETIME": 
							DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
							Date dateTime = dateTimeFormat.parse(currentColumn.get(i));
							tableDataFile.writeLong(dateTime.getTime());
							iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
							columnSchemaCounter = columnSchemaCounter + 2;
							break;
						case "DATE": 
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
							Date date = dateFormat.parse(currentColumn.get(i));
							tableDataFile.writeLong(date.getTime());
							iH.updateIndex(currentSchema, tableName, columnSchema.get(columnSchemaCounter), tableIndexPointer,columnSchema.get(columnSchemaCounter + 1), currentColumn.get(i));
							columnSchemaCounter = columnSchemaCounter + 2;
							break;
						}
					}
				}
			}			
			tableDataFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	} //End of updateTableData

	public void selectTable(int selectType, String input){
		String orgQuery = input;
		orgQuery = orgQuery.trim();
		String query = input.toLowerCase().trim();
		Schema currentSchema =  Schema.getSchemaInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();
		String whereCondition = "";
		String allowedOperator[] = {"<=",">=","<",">","="};
		String[] whereClause = new String[2];
		String workString = "";
		String columnType = "";
		boolean isColumnFound = false;
		int columnIdx = 0;

		switch(selectType){
		case 1:
			tableName = orgQuery.substring(query.indexOf("from ")+5).trim();

			if(currentSchemaName.equals("information_schema")){
				if(tableName.equals("SCHEMATA"))
					currentSchema.showSchema();
				else if(tableName.equals("TABLES"))
					currentSchema.selectISTable();
				else if(tableName.equals("COLUMNS"))
					currentSchema.selectISColumn();
			}else if(isTablePresent(currentSchemaName,tableName)){

				//Table Data Map
				TreeMap<Integer,List<String>> tableDataMap = InsertHelper.getTableData(currentSchemaName,tableName);
				Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMap.entrySet();
				Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

				//Checks the array list and writes accordingly to the File 
				while(tableDataMapIterator.hasNext()){
					Map.Entry<Integer,List<String>> columnME = tableDataMapIterator.next();
					List<String> currentRow = columnME.getValue();
					displaySelectValues(currentRow);
				}
			} 
			break;
		case 2:
			tableName = orgQuery.substring(query.indexOf("from ")+5,query.indexOf(" where")).trim();
			if(currentSchemaName.equals("information_schema")){
				if(tableName.equals("SCHEMATA"))
					currentSchema.showSchema();
				else if(tableName.equals("TABLES"))
					currentSchema.selectISTable();
				else if(tableName.equals("COLUMNS"))
					currentSchema.selectISColumn();
			} else {
				whereCondition = orgQuery.substring(query.indexOf("where ") + 6);
				String operator = "";
				int i = 0;	
				

				for(i = 0; i < allowedOperator.length; i++){
					if(whereCondition.contains(allowedOperator[i])){
						workString = (whereCondition.trim());
						break;
					}
				}
				operator = allowedOperator[i];
				whereClause = workString.split(operator);

				for(int j = 0; j < whereClause.length ;j++){
					whereClause[j] = whereClause[j].trim();
				}

				if(isTablePresent(currentSchemaName,tableName)){
					//Schema
					TableSchemaManager tsm = TableSchemaManager.getTableSchemaManagerInstance();
					TreeMap<Integer,List<String>> tableSchemaMap =	tsm.getColumnSchema(currentSchemaName, tableName);
					Set<Map.Entry<Integer,List<String>>> tableSchemaSet = tableSchemaMap.entrySet();
					Iterator<Map.Entry<Integer,List<String>>> tableSchemaIterator = tableSchemaSet.iterator();

					while(tableSchemaIterator.hasNext()){
						Map.Entry<Integer,List<String>> columnME = tableSchemaIterator.next();
						List<String> columnV = columnME.getValue();
						if(columnV.contains(whereClause[0])){
							columnIdx = columnME.getKey();
							columnType = columnV.get(1);
							isColumnFound = true;
							break;	
						} 
					}

					if(isColumnFound){
						//Table Data Map
						TreeMap<Integer,List<String>> tableDataMap = InsertHelper.getTableData(currentSchemaName,tableName);
						Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMap.entrySet();
						Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

						//Checks the array list and writes accordingly to the File 
						while(tableDataMapIterator.hasNext()){
							Map.Entry<Integer,List<String>> columnME = tableDataMapIterator.next();
							List<String> currentRow = columnME.getValue();
							switch(operator){
							case "<=":
								if(performLTETCheck(currentRow, columnIdx - 1, columnType, whereClause[1]))
									displaySelectValues(currentRow);
								break;
							case ">=":
								if(performGTETCheck(currentRow, columnIdx - 1, columnType, whereClause[1]))
									displaySelectValues(currentRow);
								break;
							case "<":
								if(performLTCheck(currentRow, columnIdx - 1, columnType, whereClause[1]))
									displaySelectValues(currentRow);
								break;
							case ">":
								if(performGTCheck(currentRow, columnIdx - 1, columnType, whereClause[1]))
									displaySelectValues(currentRow);
								break;
							case "=":
								if(performETCheck(currentRow, columnIdx - 1, columnType, whereClause[1]))
									displaySelectValues(currentRow);
								break;
							}
						} 
					}else {
						System.out.println("Unknown column '" + whereClause[0] + "' in 'where clause'");
					}
				}
			}
			break;
		case 3:
			tableName = orgQuery.substring(query.indexOf("from ")+5,query.indexOf(" where")).trim();
			if(currentSchemaName.equals("information_schema")){
				if(tableName.equals("SCHEMATA"))
					currentSchema.showSchema();
				else if(tableName.equals("TABLES"))
					currentSchema.selectISTable();
				else if(tableName.equals("COLUMNS"))
					currentSchema.selectISColumn();
			} else {
				whereCondition = orgQuery.substring(query.indexOf("where ") + 6, query.indexOf(" is")).trim();

				if(isTablePresent(currentSchemaName,tableName)){
					//Schema
					TableSchemaManager tsm = TableSchemaManager.getTableSchemaManagerInstance();
					TreeMap<Integer,List<String>> tableSchemaMap =	tsm.getColumnSchema(currentSchemaName, tableName);
					Set<Map.Entry<Integer,List<String>>> tableSchemaSet = tableSchemaMap.entrySet();
					Iterator<Map.Entry<Integer,List<String>>> tableSchemaIterator = tableSchemaSet.iterator();

					while(tableSchemaIterator.hasNext()){
						Map.Entry<Integer,List<String>> columnME = tableSchemaIterator.next();
						List<String> columnV = columnME.getValue();
						if(columnV.contains(whereCondition)){
							columnIdx = columnME.getKey();
							columnType = columnV.get(1);
							isColumnFound = true;
							break;	
						} 
					}

					if(isColumnFound){
						//Table Data Map
						TreeMap<Integer,List<String>> tableDataMap = InsertHelper.getTableData(currentSchemaName,tableName);
						Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMap.entrySet();
						Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

						//Checks the array list and writes accordingly to the File 
						while(tableDataMapIterator.hasNext()){
							Map.Entry<Integer,List<String>> columnME = tableDataMapIterator.next();
							List<String> currentRow = columnME.getValue();
							if(currentRow.get(columnIdx - 1).equalsIgnoreCase("NULL") || (currentRow.get(columnIdx - 1) == ""))
								displaySelectValues(currentRow);
						} 
					}else {
						System.out.println("Unknown column '" + whereCondition + "' in 'where clause'");
					}
				}
			}
			break;
		case 4:
			tableName = orgQuery.substring(query.indexOf("from ")+5,query.indexOf(" where")).trim();
			if(currentSchemaName.equals("information_schema")){
				if(tableName.equals("SCHEMATA"))
					currentSchema.showSchema();
				else if(tableName.equals("TABLES"))
					currentSchema.selectISTable();
				else if(tableName.equals("COLUMNS"))
					currentSchema.selectISColumn();
			} else {
				whereCondition = orgQuery.substring(query.indexOf("where ") + 6, query.indexOf(" is")).trim();

				if(isTablePresent(currentSchemaName,tableName)){
					//Schema
					TableSchemaManager tsm = TableSchemaManager.getTableSchemaManagerInstance();
					TreeMap<Integer,List<String>> tableSchemaMap =	tsm.getColumnSchema(currentSchemaName, tableName);
					Set<Map.Entry<Integer,List<String>>> tableSchemaSet = tableSchemaMap.entrySet();
					Iterator<Map.Entry<Integer,List<String>>> tableSchemaIterator = tableSchemaSet.iterator();

					while(tableSchemaIterator.hasNext()){
						Map.Entry<Integer,List<String>> columnME = tableSchemaIterator.next();
						List<String> columnV = columnME.getValue();
						if(columnV.contains(whereCondition)){
							columnIdx = columnME.getKey();
							columnType = columnV.get(1);
							isColumnFound = true;
							break;	
						} 
					}	
					if(isColumnFound){
						//Table Data Map
						TreeMap<Integer,List<String>> tableDataMap = InsertHelper.getTableData(currentSchemaName,tableName);
						Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMap.entrySet();
						Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

						//Checks the array list and writes accordingly to the File 
						while(tableDataMapIterator.hasNext()){
							Map.Entry<Integer,List<String>> columnME = tableDataMapIterator.next();
							List<String> currentRow = columnME.getValue();
							if(!(currentRow.get(columnIdx - 1).equalsIgnoreCase("NULL") || currentRow.get(columnIdx - 1) == ""))
								displaySelectValues(currentRow);
						} 
					}else {
						System.out.println("Unknown column '" + whereCondition + "' in 'where clause'");
					}
				}
			}
			break;	
		}
	} //End of selectTable

	/**
	 * Displays individual row values on the console
	 * 
	 * @param rowValues Row values in List<String> collection
	 */
	public void displaySelectValues(List<String> rowValues){
		for(int i = 0; i < rowValues.size() ; i++){
			if(i == rowValues.size() - 1)
				System.out.print(rowValues.get(i));
			else
				System.out.print(rowValues.get(i) + ",");
		}
		System.out.println();
	} //End of displaySelectValues

	/**
	 * Performing <= check 
	 * 
	 * @param row
	 * @param idx
	 * @param type
	 * @param operand
	 * @return
	 */
	public boolean performLTETCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) <= 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) <= Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) <= Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) <= Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) <= Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) <= Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) <= Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.before(dst) || src.equals(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.before(dst2) || src2.equals(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Performing >= check
	 * 
	 * @param row
	 * @param idx
	 * @param type
	 * @param operand
	 * @return
	 */
	public boolean performGTETCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) >= 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) >= Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) >= Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) >= Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) >= Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) >= Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) >= Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.after(dst) || src.equals(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.after(dst2) || src2.equals(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Performing < check
	 * 
	 * @param row
	 * @param idx
	 * @param type
	 * @param operand
	 * @return
	 */
	public boolean performLTCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) < 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) < Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) < Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) < Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) < Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) < Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) < Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.before(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.before(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Performing > check
	 * 
	 * @param row
	 * @param idx
	 * @param type
	 * @param operand
	 * @return
	 */
	public boolean performGTCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) > 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) > Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) > Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) > Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) > Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) > Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) > Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.after(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.after(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return result;
	}

	/**
	 * Performing == check
	 * 
	 * @param row
	 * @param idx
	 * @param type
	 * @param operand
	 * @return
	 */
	public boolean performETCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) == 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) == Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) == Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) == Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) == Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) == Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) == Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.equals(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.equals(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}
}
