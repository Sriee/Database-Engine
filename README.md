# Database Engine 

A rudimentary database engine implementation that is loosely based on MySQL. I will call this database as **_Base_**.

![alt Title](https://cloud.githubusercontent.com/assets/8402606/15101023/76859ed2-154a-11e6-8c7c-293f1f0a830b.GIF)

## Requirements

### Prompt 

Upon launch, your engine should present a prompt similar to the mysql> prompt, where interactive
commands may be entered.

**sql>**

### Supported Commands (Summary)

Your database engine must support the following high-level commands. All commands should be terminated by a **semicolon (;)**.

 * SHOW SCHEMAS – Displays all schemas defined in your database.
 * USE – Chooses a schema.
 * SHOW TABLES – Displays all tables in the currently chosen schema.
 * CREATE SCHEMA – Creates a new schema to hold tables.
 * CREATE TABLE – Creates a new table schema, i.e. a new empty table.
 * INSERT INTO TABLE – Inserts a row/record into a table.
 * SELECT-FROM-WHERE - style query
 * EXIT – Cleanly exits the program and saves all table and index information in non-volatile files.

#### SHOW SCHEMAS;

Display a list all database schemas by name, including the system information_schema.

##### USE _schema_name_;

This determines the schema that is currently in use (i.e. active). All other table-specific commands should consider only tables in the database schema that is currently active. When Base is launched, the currently active schema should default to **information_schema**.

##### SHOW TABLES;

Display a list all table names in the currently used schema.

##### CREATE SCHEMA schema_name;

Create a new schema.

##### CREATE TABLE

	**CREATE TABLE** table_name (
	column_name1 data_type(size) [primary key|not null],
	column_name2 data_type(size) [primary key|not null],
	column_name3 data_type(size) [primary key|not null],
	...
	);

Create the table schema information for a new table. It will be created in the current schema. In other words, add appropriate entries to the system **information_schema** tables that define the described **CREATE TABLE**.

Your table definition should support the following data types. All numbers should be represented as bytes in _Big Endian_ order.

| Datatype  | Data size (bytes) | 
| ----------| ------------------| 
| BYTE 		| 1 				|
| SHORT 	| 2 				|
| INT 		| 4 				|
| LONG		| 8					|
| CHAR(n) 	| n					|
| VARCHAR(n)| _variable_		|
| FLOAT 	| 4 				|
| DOUBLE 	| 8 				|
| DATETIME 	| 8					|
| DATE 		| 8 				|

The only table constraints that are support are PRIMARY KEY and NOT NULL (to indicate that NULL values are not permitted for a particular column). All primary keys are single column keys. If a column is a primary key, its **information_schema.COLUMNS.COLUMN_KEY** attribute will be **“PRI”**, otherwise, it will be the empty string. If a column is defined as **NOT NULL**, then its **information_schema.COLUMNS.IS_NULLABLE** attribute will be **“NO”**, otherwise, it will be **“YES”**. Base does not support **FOREIGN KEY**.

##### INSERT INTO TABLE

	INSERT INTO TABLE table_name VALUES (value1,value2,value3,…);

Insert a new record into the indicated table. If n values are supplied, they will be mapped onto the first n columns. Prohibit inserts that do not include
the primary key column or do not include a NOT NULL column. For columns that allow NULL values, INSERT INTO TABLE should parse the keyword NULL in the values list as the special value NULL.

##### SELECT-FROM-WHERE

	SELECT *
	FROM table_name
	WHERE column_name operator value;
	
Query syntax is similar to formal SQL. The result set should display to stdout (the terminal) formatted like a typical SQL query. The differences between Base query syntax and SQL query syntax is described below.

 * SELECT only needs to support the * wildcard, which will display all columns in **ORDINAL_POSITION** order.
 * You only need to support one filter condition in the WHERE clause. Note that the WHERE clause is optional (as in MySQL). 

### File Formats
 
Both table data and index data must be saved to files so that your database state is preserved after you exit the database. When you re-launch Base, your database engine should load the previous state from table data and index files.

#### Table Files

Tables files should store table data in binary format. Table files should not include any delimiters between records or between columns. i.e. No linefeeds **(\n)**. No carriage returns **(\r)**. No string terminators **(\0)**. Table data files should use the naming convention: **_schema_name.table_name_.tbl**.

#### Index Files

Index files must be created for all columns in a table. This allows efficient search (binary lookup) on any field. Therefore, each table insert should append a new record to the end of a data file and concurrently update all associated index files.

Index files should use the naming convention: **_schema_name.table_name.column_name_.ndx**.

The file format must be binary with each index entry being a **key-value** pair. The key is the column value. The value is a list of location(s) where the associated record is in the data file. Each record location in the list is a 4-byte integer that indicates the number of bytes offset from the beginning of the data file. The value list begins with a 4-byte integer that indicates how many values follow.

## Licence 

This project is licensed under the MIT License - see the [LICENCE](../master/LICENSE) file for details
 
