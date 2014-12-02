package com.awesome.pro.db.cassandra.client;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.awesome.pro.db.cassandra.references.CassandraClientReferences;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Contains common methods that can be invoked to connect to Cassandra server.
 * @author siddharth.s
 */
public class CassandraClientManager {

	/**
	 * Map of key space name to context.
	 */
	private static final Map<String, AstyanaxContext<Keyspace>> CONTEXT = new LinkedHashMap<>();

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			CassandraClientManager.class);

	/**
	 * Initialize external configurations.
	 * @param configFile Path and name of configuration file.
	 */
	public static final void initialize(final String configFile) {
		CassandraClientReferences.initialize(configFile);
		LOGGER.info("Intialized Cassandra client configurations.");
	}

	/**
	 * Shuts down all initialized contexts.
	 */
	public static final void shutdown() {
		LOGGER.info("Shutting down Cassandra client pool.");
		final Iterator<AstyanaxContext<Keyspace>> iter =
				CONTEXT.values().iterator();
		while (iter.hasNext()) {
			final AstyanaxContext<Keyspace> context = iter.next();
			context.shutdown();
		}
	}

	/**
	 * @param keyspaceName Name of the key space.
	 * @return Reference to the specified key space
	 */
	public static final Keyspace getKeyspace(final String keyspaceName) {
		if (!CONTEXT.containsKey(keyspaceName)) {
			synchronized (CassandraClientManager.class) {
				if (!CONTEXT.containsKey(keyspaceName)) {
					initializeContext(keyspaceName);
				}
			}
		}
		return CONTEXT.get(keyspaceName).getClient();
	}

	/**
	 * @param columnFamilyName Name of the column family.
	 * @return Reference to the column family.
	 */
	public static final ColumnFamily<String, String> getColumnFamily(
			final String columnFamilyName) {
		return ColumnFamily.newColumnFamily(columnFamilyName,
				StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
	}

	/**
	 * Initialize connection pool context.
	 * @param keyspace Name of the key space to connect to.
	 */
	private static synchronized final void initializeContext(final String keyspace) {
		LOGGER.info("Initializing a new context for keyspace: " + keyspace);
		final AstyanaxContext<Keyspace> context =
				new AstyanaxContext.Builder().
				forCluster(CassandraClientReferences.CONFIG.getStringValue(
						CassandraClientReferences.PARAMETER_CLUSTER_NAME,
						CassandraClientReferences.DEFAULT_CLUSTER_NAME)
						)
						.forKeyspace(keyspace)
						.withAstyanaxConfiguration(
								new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
								.setCqlVersion(CassandraClientReferences.CONFIG.getStringValue(
										CassandraClientReferences.PARAMETER_CQL_VERSION,
										CassandraClientReferences.DEFAULT_CQL_VERSION)
										)
										.setTargetCassandraVersion(CassandraClientReferences.CONFIG.getStringValue(
												CassandraClientReferences.PARAMETER_CASSANDRA_VERSION,
												CassandraClientReferences.DEFAULT_CASSANDRA_VERSION)
												)
								)
								.withConnectionPoolConfiguration(
										new ConnectionPoolConfigurationImpl(CassandraClientReferences.CONNECTION_POOL_NAME)
										.setInitConnsPerHost(CassandraClientReferences.CONFIG.getIntegerValue(
												CassandraClientReferences.PARAMETER_INITIAL_CONNECTIONS,
												CassandraClientReferences.DEFAULT_INITIAL_CONNECTIONS)
												)
												.setMaxConnsPerHost(CassandraClientReferences.CONFIG.getIntegerValue(
														CassandraClientReferences.PARAMETER_MAX_CONNECTIONS,
														CassandraClientReferences.DEFAULT_MAX_CONNECTIONS)
														)
														.setSeeds(CassandraClientReferences.CONFIG.getStringValue(
																CassandraClientReferences.PARAMETER_SEEDS,
																CassandraClientReferences.DEFAULT_SEEDS)
																)
										)
										.withConnectionPoolMonitor(new CountingConnectionPoolMonitor()												)
										.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		CONTEXT.put(keyspace, context);
	}

}
