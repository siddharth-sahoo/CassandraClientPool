package com.awesome.pro.db.cassandra.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.awesome.pro.db.cassandra.references.CassandraClientReferences;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

/**
 * General utility methods for Cassandra clients.
 * @author siddharth.s
 */
public class CassandraUtilities {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			CassandraUtilities.class);

	/**
	 * Creates a new key space, assuming replication factor as 1
	 * and using simple strategy.
	 * @param name Name of the key space to be created.
	 * @return Key space client pool reference.
	 */
	public static final Keyspace createKeyspace(final String name) {
		final Keyspace keyspace = CassandraClientManager.getKeyspace(name);
		try {
			keyspace.createKeyspaceIfNotExists(
					CassandraClientReferences.KEYSPACE_CREATION_OPTIONS);
		} catch (ConnectionException e) {
			LOGGER.error("Cassandra client error.", e);
			return null;
		}

		return keyspace;
	}

	/**
	 * @param name Name of the key space to be dropped.
	 */
	public static final void dropKeyspace(final String name) {
		LOGGER.info("Dropping key space: " + name);
		final Keyspace keyspace = CassandraClientManager.getKeyspace(name);
		try {
			keyspace.dropKeyspace();
		} catch (ConnectionException e) {
			LOGGER.error("Cassandra client error.", e);
			return;
		}
	}

	/**
	 * @param keyspaceName Name of the key space where the column family
	 * is to be created.
	 * @param columnFamilyName Name of the column family to be created.
	 * @return Reference to the column family. Null if there is an error.
	 */
	public static final ColumnFamily<String, String> createColumnFamily(final String keyspaceName,
			final String columnFamilyName) {
		final ColumnFamily<String, String> columnFamily =
				CassandraClientManager.getColumnFamily(columnFamilyName);
		try {
			CassandraClientManager.getKeyspace(keyspaceName)
			.createColumnFamily(columnFamily,
					CassandraClientReferences.COLUMN_FAMILY_OPTIONS);
			return columnFamily;
		} catch (ConnectionException | NullPointerException e) {
			LOGGER.error("Error in creating column family.", e);
			return null;
		}
	}

	/**
	 * @param keyspaceName Name of the key space where the column
	 * family is present.
	 * @param columnFamilyName Name of the column family to be
	 * dropped.
	 */
	public static final void dropColumnFamily(final String keyspaceName,
			final String columnFamilyName) {
		try {
			CassandraClientManager.getKeyspace(keyspaceName)
			.dropColumnFamily(columnFamilyName);
		} catch (ConnectionException e) {
			LOGGER.error("Unable to drop column family.", e);
		}
	}

	/**
	 * @param keyspaceName Name of the key space.
	 * @param columnFamilyName Name of the column family.
	 * @param rowKey Row key to query for.
	 * @return Columns corresponding to the row key. Null if there is an error.
	 */
	public static final ColumnList<String> queryRow(final String keyspaceName,
			final String columnFamilyName, final String rowKey) {
		try {
			return CassandraClientManager.getKeyspace(keyspaceName)
					.prepareQuery(CassandraClientManager
							.getColumnFamily(columnFamilyName))
							.getKey(rowKey).execute().getResult();
		} catch (ConnectionException | NullPointerException e) {
			LOGGER.error("Error in executing query.", e);
			return null;
		}
	}

	/**
	 * @param keyspaceName Name of the key space.
	 * @param columnFamilyName Name of the column family.
	 * @param rowKey Row key to query for.
	 * @param columnName Name of the column to be retrieved.
	 * @return Column result for the specified row key. Null if there is an error.
	 */
	public static final Column<String> queryRowByColumn(final String keyspaceName,
			final String columnFamilyName, final String rowKey,
			final String columnName) {
		try {
			return CassandraClientManager.getKeyspace(keyspaceName)
					.prepareQuery(CassandraClientManager
							.getColumnFamily(columnFamilyName))
							.getKey(rowKey)
							.getColumn(columnName)
							.execute().getResult();
		} catch (ConnectionException | NullPointerException e) {
			LOGGER.error("Error in executing the query.", e);
			return null;
		}
	}

	/**
	 * @param keyspaceName Name of the key space.
	 * @param columnFamilyName Name of the column family.
	 * @return All rows in the column family. Null if there is an error.
	 */
	public static final Rows<String, String> queryAllRows(final String keyspaceName,
			final String columnFamilyName) {
		try {
			return CassandraClientManager.getKeyspace(keyspaceName)
					.prepareQuery(CassandraClientManager
							.getColumnFamily(columnFamilyName))
							.getAllRows().execute().getResult();
		} catch (ConnectionException | NullPointerException e) {
			LOGGER.error("Error in executing the query.", e);
			return null;
		}
	}

	/**
	 * @param keyspaceName Name of the key space to store data in.
	 * @param columnFamilyName Name of the column family in the
	 * specified key space.
	 * @param rowKey Row key to store data in.
	 * @param columnName Name of the column to store data in for the
	 * specified row key.
	 * @param value Value to store in the column.
	 */
	public static final void storeData(final String keyspaceName,
			final String columnFamilyName, final String rowKey,
			final String columnName, final String value) {
		try {
			CassandraClientManager.getKeyspace(keyspaceName)
			.prepareColumnMutation(
					CassandraClientManager.getColumnFamily(columnFamilyName),
					rowKey, columnName)
					.putValue(value, null).execute();
		} catch (ConnectionException e) {
			LOGGER.error("Unable to store data.", e);
		}
	}

	/**
	 * @param keyspaceName Name of the key space to store data in.
	 * @param values Map specifying the values to be inserted.
	 * Key is pair of column family name and row key.
	 * Value is map of column name to corresponding value.
	 */
	public static final void storeData(final String keyspaceName,
			final Map<Entry<String, String>, Map<String, String>> values) {
		final MutationBatch mutationBatch = CassandraClientManager
				.getKeyspace(keyspaceName).prepareMutationBatch();

		final Iterator<Entry<Entry<String, String>, Map<String, String>>> iter =
				values.entrySet().iterator();

		while (iter.hasNext()) {
			final Entry<Entry<String, String>, Map<String, String>> entry =
					iter.next();

			// final String columnFamilyName = entry.getKey().getKey();
			// final String rowKey = entry.getKey().getValue();

			final Iterator<Entry<String, String>> columnIter =
					entry.getValue().entrySet().iterator();

			while (columnIter.hasNext()) {
				final Entry<String, String> columnEntry = columnIter.next();
				mutationBatch.withRow(
						CassandraClientManager.getColumnFamily(entry.getKey().getKey()),
						entry.getKey().getValue())
						.putColumn(columnEntry.getKey(), columnEntry.getValue(), null);
			}
		}

		try {
			mutationBatch.execute();
		} catch (ConnectionException e) {
			LOGGER.error("Unable to execute batch mutation.", e);
		}
	}

	/**
	 * @param keyspaceName Name of the key space to store data in.
	 * @param columnFamilyName Name of the column family to store data in.
	 * @param values Map specifying the values to be inserted.
	 * Key is pair of column family name and row key.
	 * Value is map of column name to corresponding value.
	 */
	public static final void storeData(final String keyspaceName,
			final String columnFamilyName, 
			final Map<String, Map<String, String>> values) {

		final MutationBatch mutationBatch = CassandraClientManager
				.getKeyspace(keyspaceName).prepareMutationBatch();

		final Iterator<Entry<String, Map<String, String>>> iter = values.entrySet().iterator();

		while (iter.hasNext()) {
			final Entry<String, Map<String, String>> entry = iter.next();

			// final String rowKey = entry.getKey();

			final Iterator<Entry<String, String>> columnIter =
					entry.getValue().entrySet().iterator();

			while (columnIter.hasNext()) {
				final Entry<String, String> columnEntry = columnIter.next();
				mutationBatch.withRow(
						CassandraClientManager.getColumnFamily(columnFamilyName),
						entry.getKey())
						.putColumn(columnEntry.getKey(), columnEntry.getValue(), null);
			}
		}

		try {
			mutationBatch.execute();
		} catch (ConnectionException e) {
			LOGGER.error("Unable to execute batch mutation.", e);
		}
	}

	/**
	 * @param keyspaceName Name of the key space to insert into.
	 * @param columnFamilyName Name of the column family in the key space.
	 * @param rowKey Row key to insert for.
	 * @param values Map of column names and respective values.
	 */
	public static final void storeData(final String keyspaceName,
			final String columnFamilyName, final String rowKey,
			final Map<String, String> values) {

		final MutationBatch mutationBatch = CassandraClientManager
				.getKeyspace(keyspaceName).prepareMutationBatch();

		ColumnListMutation<String> columnListMutation = mutationBatch
				.withRow(CassandraClientManager.
						getColumnFamily(columnFamilyName),
						rowKey);

		final Iterator<Entry<String, String>> iter = values.entrySet().iterator();
		while (iter.hasNext()) {
			final Entry<String, String> entry = iter.next();
			columnListMutation = columnListMutation.putColumn(
					entry.getKey(), entry.getValue(), null);
		}

		try {
			mutationBatch.execute();
		} catch (ConnectionException e) {
			LOGGER.error("Unable to write to Cassandra.", e);
		}
	}

	/**
	 * Deletes a single column for a single row.
	 * @param keyspaceName Name of the key space to delete the data in.
	 * @param columnFamilyName Name of the column family in the specified key space.
	 * @param rowKey Roe key to delete column from.
	 * @param columnName Name of the column to delete.
	 */
	public static final void deleteData(final String keyspaceName,
			final String columnFamilyName, final String rowKey,
			final String columnName) {
		try {
			CassandraClientManager.getKeyspace(keyspaceName)
			.prepareColumnMutation(
					CassandraClientManager.getColumnFamily(columnFamilyName),
					rowKey, columnName)
					.deleteColumn().execute();
		} catch (ConnectionException e) {
			LOGGER.error("Unable to store data.", e);
		}
	}

	/**
	 * Deletes specified columns for multiple rows.
	 * @param keyspaceName Name of the key space to delete from.
	 * @param columns Map specifying keys and columns to delete.
	 * Key is pair of column family name and row key.
	 * Value is set of column names to delete.
	 */
	public static final void deleteData(final String keyspaceName,
			final Map<Entry<String, String>, Set<String>> columns) {
		final MutationBatch mutationBatch = CassandraClientManager
				.getKeyspace(keyspaceName).prepareMutationBatch();

		final Iterator<Entry<Entry<String, String>, Set<String>>> iter =
				columns.entrySet().iterator();

		while (iter.hasNext()) {
			final Entry<Entry<String, String>, Set<String>> entry = iter.next();

			// final String columnFamilyName = entry.getKey().getKey();
			// final String rowKey = entry.getKey().getValue();

			final Iterator<String> columnIter = entry.getValue().iterator();
			while (columnIter.hasNext()) {
				final String column = columnIter.next();
				mutationBatch.withRow(
						CassandraClientManager.getColumnFamily(
								entry.getKey().getKey()),
								entry.getKey().getValue())
								.deleteColumn(column);
			}
		}

		try {
			mutationBatch.execute();
		} catch (ConnectionException e) {
			LOGGER.error("Unable to execute batch mutation.", e);
		}
	}

	/**
	 * Deletes all columns for the specified row keys.
	 * @param keyspaceName Name of the key space to delete from.
	 * @param rowKeys Map of column family name to set of row keys.
	 */
	public static final void deleteAllColumns(final String keyspaceName,
			final Map<String, Set<String>> rowKeys) {
		final MutationBatch mutationBatch = CassandraClientManager
				.getKeyspace(keyspaceName).prepareMutationBatch();

		final Iterator<Entry<String, Set<String>>> iter =
				rowKeys.entrySet().iterator();
		while (iter.hasNext()) {
			final Entry<String, Set<String>> entry = iter.next();
			// final String columnFamilyName = entry.getKey();

			final Iterator<String> rowKeyIter = entry.getValue().iterator();
			while (rowKeyIter.hasNext()) {
				final String rowKey = rowKeyIter.next();
				mutationBatch.withRow(
						CassandraClientManager.getColumnFamily(entry.getKey()),
						rowKey).delete();
			}

			try {
				mutationBatch.execute();
			} catch (ConnectionException e) {
				LOGGER.error("Error in executing batch mutation.", e);
			}
		}
	}

	/**
	 * @param fileName Name of the file to write to.
	 * @param keySpaceName Name of the key space to query.
	 * @param columnFamilyName Name of the column family to retrieve data from.
	 */
	public static final void printRowsToFile(final String fileName,
			final String keySpaceName, final String columnFamilyName) {
		printRowsToFile(fileName, queryAllRows(keySpaceName,
				columnFamilyName));
	}

	/**
	 * @param fileName Path and name of file to be written to.
	 * @param rows Data to be written.
	 */
	public static final void printRowsToFile(final String fileName,
			final Rows<String, String> rows) {
		final List<String> columns = new LinkedList<>();
		final Iterator<Row<String, String>> iter = rows.iterator();

		try {
			final BufferedWriter writer = new BufferedWriter(
					new FileWriter(fileName, false));

			while (iter.hasNext()) {
				final ColumnList<String> row = iter.next().getColumns();

				// Track column names.
				final Iterator<String> columnIter = row
						.getColumnNames().iterator();
				while (columnIter.hasNext()) {
					final String column = columnIter.next();
					if (!columns.contains(column)) {
						columns.add(column);
					}
				}

				final int columnCount = columns.size();
				for (int i = 0; i < columnCount; i ++) {
					writer.write(row.getColumnByName(columns.get(i))
							.getStringValue() + ',');
				}
				writer.write('\n');
			}
			
			final int columnCount = columns.size();
			for (int i = 0; i < columnCount; i ++) {
				writer.write(columns.get(i) + ',');
			}
			
			writer.close();
		} catch (IOException e) {
			LOGGER.error("Unable to write to file.", e);
			System.exit(1);
		}
	}

}
