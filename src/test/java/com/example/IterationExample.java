package com.example;

import java.util.Iterator;

import com.awesome.pro.db.cassandra.client.CassandraUtilities;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class IterationExample {
	
	public static void main(String[] args) {
		Rows<String, String> rows = CassandraUtilities.queryAllRows("Key space name", "column family name");
		Iterator<Row<String, String>> iter = rows.iterator();
		while (iter.hasNext()) {
			Row<String, String> row = iter.next();
			ColumnList<String> columns = row.getColumns();
			int size = columns.size();
			for (int i = 0; i < size; i ++) {
				Column<String> column = columns.getColumnByIndex(i);
				column.getName();
				column.getStringValue();
			}
		}
	}

}
