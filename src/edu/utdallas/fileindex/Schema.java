package edu.utdallas.fileindex;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


public class Schema {

	/**
	 * Singleton Design Pattern to share schema current instance accross other classes
	 */
	private Schema(){}

	private static Schema _schema;

	public static Schema getSchemaInstance(){
		if(_schema == null)
			return new Schema();

		return _schema;
	}

	public static String currentSchema = "information_schema"; //Default schema name

	/**
	 * Displays the list of schema's present in the schemata table
	 */
	public void showSchema(){
		int schemaCount = 1;
		try {
			RandomAccessFile schemataFile = new RandomAccessFile("information_schema.schemata.tbl", "rw");

			System.out.println("------------------------------------------------");
			System.out.println("No.\tSchema Name");
			System.out.println("------------------------------------------------");


			while(schemataFile.getFilePointer() < schemataFile.length()){
				System.out.print(schemaCount++ + "\t");
				byte varcharLength = schemataFile.readByte();
				for(int j = 0; j < varcharLength; j++)
					System.out.print((char)schemataFile.readByte());
				System.out.print("\n");
			}

			System.out.println("------------------------------------------------\n");

			//Closing the file 
			schemataFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (EOFException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}  catch (Exception e){
			e.printStackTrace();
		}	
	}

	/**
	 * Setter method for current schema name
	 * 
	 * @param schema The name of the schema chosen by the user
	 */
	public void setCurrentSchema(String schema){
		Schema.currentSchema = schema;
	}

	/**
	 * Getter method to get the current schema name
	 * 
	 * @return The name of the current schema 
	 */
	public String getCurrentSchema(){
		return Schema.currentSchema;
	}

	/**
	 * Sets the current schema to the schema name inputed by the user
	 * 
	 * @param query Use SCHEMA_NAME from user 
	 */
	public void useSchema(String input){
		String query = input;
		query = query.trim();
		input = input.toLowerCase().trim();
		String schemaName = query.substring(input.indexOf("use ")+4,input.length()).trim();
		boolean isFound = true;
		try {

			RandomAccessFile schemaFile = new RandomAccessFile("information_schema.schemata.tbl","r");

			while(schemaFile.getFilePointer() < schemaFile.length()){
				String readSchemaName = "";
				byte varcharLength = schemaFile.readByte();
				for(int j = 0; j < varcharLength; j++)
					readSchemaName += (char)schemaFile.readByte();
				if(readSchemaName.equals(schemaName)){
					this.setCurrentSchema(schemaName);
					System.out.println("Current Schema: " + this.getCurrentSchema() + "\nDatabase Changed....");
					schemaFile.close();
					isFound = false;
					break;
				} 
				
			}

			if(isFound){
				System.out.println("Schema File Not found!...");
				schemaFile.close();
				return;
			} 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	/**
	 * Creates a schema
	 * 
	 * @param query The name of the new schema
	 */
	public void createSchema(String input){
		String query = input;
		query = query.trim();
		input = input.toLowerCase().trim();
		String newSchemaName = query.substring(input.indexOf("schema ")+7,input.length()).trim();
		boolean isFound = false;
		try {

			RandomAccessFile newSchemaFile = new RandomAccessFile("information_schema.schemata.tbl","rw");

			//Searching to see if the information schema is present or not
			while(newSchemaFile.getFilePointer() < newSchemaFile.length()){
				String readSchemaName = "";
				byte varcharLength = newSchemaFile.readByte();
				for(int j = 0; j < varcharLength; j++)
					readSchemaName += (char)newSchemaFile.readByte();

				if(readSchemaName.equals(newSchemaName)){
					System.out.println("Can't create database '" + newSchemaName + "'\nDatabase exists...");
					isFound = true;
					break;
				} 	
			}

			newSchemaFile.seek(newSchemaFile.length());
			if(!isFound){
				newSchemaFile.writeByte(newSchemaName.length());
				newSchemaFile.writeBytes(newSchemaName);
				System.out.println("Schema '" + newSchemaName + "'created successfully...");
			}

			//Closing the file 
			newSchemaFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	/**
	 * Select * from information_schema.table printing function  
	 */
	public void selectISTable(){
		try {
			RandomAccessFile tableFile = new RandomAccessFile("information_schema.table.tbl", "rw");

			while ( tableFile.getFilePointer() < tableFile.length() ) {
				String readSchemaName = "";
				String readTableName = "";
				int numberOfRows = 0;
				byte varcharLength = tableFile.readByte();
				for(int j = 0; j < varcharLength; j++)
					readSchemaName += (char)tableFile.readByte();

				byte vartableLength = tableFile.readByte();
				for(int k = 0; k < vartableLength; k++)
					readTableName += (char)tableFile.readByte();

				numberOfRows = (int)tableFile.readLong();

				System.out.println(readSchemaName + "," + readTableName + "," + numberOfRows);
			}
			tableFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Select * from information_schema.columns printing function 
	 */
	public void selectISColumn(){
		try {
			RandomAccessFile columnsFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
			while ( columnsFile.getFilePointer() < columnsFile.length() ) {
				String readSchemaName = "";
				String readTableName = "";
				String readColumnName = "";
				int ordinalPosition = 0;
				String columnType = "";
				String isNullable = "";
				String isPrimaryKey = "";
				
				// TABLE_SCHEMA
				byte varcharLength = columnsFile.readByte();
				for(int j = 0; j < varcharLength; j++)
					readSchemaName += (char)columnsFile.readByte();
				
				// TABLE_NAME
				byte vartableLength = columnsFile.readByte();
				for(int k = 0; k < vartableLength; k++)
					readTableName += (char)columnsFile.readByte();

				// COLUMN_NAME
				byte varColumnLength = columnsFile.readByte();
				for(int k = 0; k < varColumnLength; k++)
					readColumnName += (char)columnsFile.readByte();
				
				// ORDINAL_POSITION
				ordinalPosition = columnsFile.readInt();

				// COLUMN_TYPE
				byte varTypeLength = columnsFile.readByte();
				for(int k = 0; k < varTypeLength; k++)
					columnType += (char)columnsFile.readByte();
				
				// IS_NULLABLE
				byte varNullLength = columnsFile.readByte();
				for(int k = 0; k < varNullLength; k++)
					isNullable += (char)columnsFile.readByte();
				
				// COLUMN_KEY
				byte varPKLength = columnsFile.readByte();
				for(int k = 0; k < varPKLength; k++)
					isPrimaryKey += (char)columnsFile.readByte();	
				if(isPrimaryKey.equals("")) isPrimaryKey = "NO";
				
				System.out.println(readSchemaName + "," + readTableName + "," +readColumnName + "," +ordinalPosition + "," +columnType + "," +isNullable + "," +isPrimaryKey);
			}
			columnsFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Initialized Information schema table
	 * 
	 * @return isInitialized True if the information schema tables are initialized successfully
	 */
	public boolean initInformationSchema(){
		boolean isInitialized = false;
		try {
			RandomAccessFile schemataTableFile = new RandomAccessFile("information_schema.schemata.tbl", "rw");
			RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema.table.tbl", "rw");
			RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");

			// ROW 1: information_schema.schemata.tbl
			schemataTableFile.writeByte("information_schema".length());
			schemataTableFile.writeBytes("information_schema");

			// ROW 1: information_schema.tables.tbl
			tablesTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			tablesTableFile.writeBytes("information_schema");
			tablesTableFile.writeByte("SCHEMATA".length()); // TABLE_NAME
			tablesTableFile.writeBytes("SCHEMATA");
			tablesTableFile.writeLong(1); // TABLE_ROWS

			// ROW 2: information_schema.tables.tbl
			tablesTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			tablesTableFile.writeBytes("information_schema");
			tablesTableFile.writeByte("TABLES".length()); // TABLE_NAME
			tablesTableFile.writeBytes("TABLES");
			tablesTableFile.writeLong(3); // TABLE_ROWS

			// ROW 3: information_schema.tables.tbl
			tablesTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			tablesTableFile.writeBytes("information_schema");
			tablesTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			tablesTableFile.writeBytes("COLUMNS");
			tablesTableFile.writeLong(7); // TABLE_ROWS

			/*
			 *  Create the COLUMNS table file.
			 *  Initially it has 11 rows:
			 */
			// ROW 1: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("SCHEMATA".length()); // TABLE_NAME
			columnsTableFile.writeBytes("SCHEMATA");
			columnsTableFile.writeByte("SCHEMA_NAME".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("SCHEMA_NAME");
			columnsTableFile.writeInt(1); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 2: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("TABLES".length()); // TABLE_NAME
			columnsTableFile.writeBytes("TABLES");
			columnsTableFile.writeByte("TABLE_SCHEMA".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_SCHEMA");
			columnsTableFile.writeInt(1); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 3: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("TABLES".length()); // TABLE_NAME
			columnsTableFile.writeBytes("TABLES");
			columnsTableFile.writeByte("TABLE_NAME".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_NAME");
			columnsTableFile.writeInt(2); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 4: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("TABLES".length()); // TABLE_NAME
			columnsTableFile.writeBytes("TABLES");
			columnsTableFile.writeByte("TABLE_ROWS".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_ROWS");
			columnsTableFile.writeInt(3); // ORDINAL_POSITION
			columnsTableFile.writeByte("long int".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("long int");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 5: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");
			columnsTableFile.writeByte("TABLE_SCHEMA".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_SCHEMA");
			columnsTableFile.writeInt(1); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 6: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");
			columnsTableFile.writeByte("TABLE_NAME".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_NAME");
			columnsTableFile.writeInt(2); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 7: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");
			columnsTableFile.writeByte("COLUMN_NAME".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("COLUMN_NAME");
			columnsTableFile.writeInt(3); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 8: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");
			columnsTableFile.writeByte("ORDINAL_POSITION".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("ORDINAL_POSITION");
			columnsTableFile.writeInt(4); // ORDINAL_POSITION
			columnsTableFile.writeByte("int".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("int");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 9: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");
			columnsTableFile.writeByte("COLUMN_TYPE".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("COLUMN_TYPE");
			columnsTableFile.writeInt(5); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 10: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");
			columnsTableFile.writeByte("IS_NULLABLE".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("IS_NULLABLE");
			columnsTableFile.writeInt(6); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(3)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(3)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 11: information_schema.columns.tbl
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");
			columnsTableFile.writeByte("COLUMN_KEY".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("COLUMN_KEY");
			columnsTableFile.writeInt(7); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(3)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(3)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");


			//Closing RandomAccessFiles
			isInitialized = true;
			schemataTableFile.close();
			tablesTableFile.close();
			columnsTableFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}  

		return isInitialized;
	}

}