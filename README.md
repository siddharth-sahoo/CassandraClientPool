CassandraClientPool
===================

Pooled Cassandra client and abstracted common operations.

* Startup/shutdown:

````java
CassandraClientManager.initialize("< config file name >");

CassandraClientManager.shutdown();
````

* Create key space:

````java
CassandraUtilities.createKeyspace("keyspace_test");
````

* Create column family:

````java
CassandraUtilities.createColumnFamily("keyspace_test", "column_family_test");
````

* Store data:

````java
CassandraUtilities.storeData("keyspace_test", "column_family_test", "row_key_test",
  "column_test", "column_value_test");
````

* Retrieve all rows in a column family:

````java
Rows<String, String> rows = CassandraUtilities.queryAllRows(
  "keyspace_test", "column_family_test");
Iterator<Row<String, String>> rowIter = rows.iterator();

// Iterate over rows
while (rowIter.hasNext()) {
  Row<String, String> row = rowIter.next();
  
  // Get row key
  row.getKey();
  
  ColumnList<String> columns = row.getColumns();
  
  //Get column names
  columns.getColumnNames();
  
  Iterator<Column<String>> columnIter = columns.iterator();
  
  // Iterate over columns
  while (columnIter.hasNext()) {
    Column<String> column = columnIter.next();
    
    // Get column name
    column.getName();
    
    // Get column value
    column.getStringValue();
  }
}
````

* Retrieve a particular row:

````java
ColumnList<String> columns = CassandraUtilities.queryRow(
  "keyspace_test", "column_family_test", "row_key_test");
````

* Retrieve a particular column of a particular row:

````java
Column<String> column = CassandraUtilities.queryRowByColumn(
  "keyspace_test", "column_family_test", "row_key_test", "column_test");
````