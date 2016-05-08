package edu.utdallas.fileindex;

public class CreateTableHelper{

	private String columnName = null;
	private String dataType = null;
	private boolean isPrimaryKey = false;
	private boolean isNull = false;

	/**
	 * @return COLUMN_NAME
	 */
	public String getColumnName() {
		return columnName;
	}
	
	/**
	 * @param columnName Sets the column name
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	/**
	 * @return DATA_TYPE
	 */
	public String getDataType() {
		return dataType;
	}
	
	/**
	 * @param dataType Sets the data type for the current column name
	 */
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	
	/**
	 * @return true if the column is a PRIMARY_KEY
	 * @return false if not 
	 */
	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}
	
	/**
	 * Sets true if the column is primary key 
	 * @param isPrimaryKey
	 */ 
	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}
	
	/**
	 * @return true if the column constraint is NOT NULL 
	 * @return false if not
	 */
	public boolean isNull() {
		return isNull;
	}
	
	/**
	 * Sets true if the NOT NULL constraint is applied
	 * @param isNull
	 */
	public void setNull(boolean isNull) {
		this.isNull = isNull;
	}
}
