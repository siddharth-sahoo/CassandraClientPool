package com.awesome.pro.db.cassandra.references;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.awesome.pro.utilities.PropertyFileUtility;

public class CassandraClientReferences {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			CassandraClientReferences.class);

	/**
	 * Initialize external configurations.
	 * @param configFile Path and name of configuration file.
	 */
	public static final void initialize(final String configFile) {
		if (configFile == null) {
			LOGGER.error("Null configuration file is specified.");
			System.exit(1);
		}

		if (CONFIG == null) {
			synchronized (CassandraClientReferences.class) {
				if (CONFIG == null) {
					CONFIG = new PropertyFileUtility(configFile);
				}
			}
		}
	}

	/**
	 * Cassandra configurations.
	 */
	public static PropertyFileUtility CONFIG = null;
	public static final String FILE_CASSANDRA_CONFIG = "cassandra.properties";

	public static final String CONNECTION_POOL_NAME = "ConnPool";

	// Cassandra Client Configuration Parameters.
	public static final String PARAMETER_CLUSTER_NAME = "ClusterName";
	public static final String PARAMETER_SEEDS = "Seeds";
	public static final String PARAMETER_CQL_VERSION = "CQLVersion";
	public static final String PARAMETER_CASSANDRA_VERSION = "CassandraVersion";
	public static final String PARAMETER_INITIAL_CONNECTIONS = "InitialConnectionsPerHost";
	public static final String PARAMETER_MAX_CONNECTIONS = "MaxConnectionsPerHost";

	// Cassandra Client Default Configurations.
	public static final String DEFAULT_CLUSTER_NAME = "Test Cluster";
	public static final String DEFAULT_SEEDS = "localhost:9160";
	public static final String DEFAULT_CQL_VERSION = "3.0.0";
	public static final String DEFAULT_CASSANDRA_VERSION = "2.0.2.1";
	public static final int DEFAULT_INITIAL_CONNECTIONS = 20;
	public static final int DEFAULT_MAX_CONNECTIONS = 50;

	// Key space configurations.
	public static final String KEYSPACE_STRATEGY_OPTIONS = "strategy_options";
	public static final String KEYSPACE_STRATEGY_CLASS = "strategy_class";
	public static final String DEFAULT_STRATEGY_CLASS = "SimpleStrategy";
	public static final String KEYSPACE_REPLICATION_FACTOR = "replication_factor";
	public static final String DEFAULT_REPLICATION_FACTOR = "1";

	private static final Map<String, Object> DEFAULT_KEYSPACE_STRATEGY_OPTIONS = new HashMap<>();
	static {
		DEFAULT_KEYSPACE_STRATEGY_OPTIONS.put(KEYSPACE_REPLICATION_FACTOR,
				DEFAULT_REPLICATION_FACTOR);
	}
	
	public static final Map<String, Object> KEYSPACE_CREATION_OPTIONS = new HashMap<>();
	static {
		KEYSPACE_CREATION_OPTIONS.put(KEYSPACE_STRATEGY_OPTIONS, DEFAULT_KEYSPACE_STRATEGY_OPTIONS);
		KEYSPACE_CREATION_OPTIONS.put(KEYSPACE_STRATEGY_CLASS, DEFAULT_STRATEGY_CLASS);
	}

	// Column family creation options.
	public static final String CF_KEY_CLASS = "key_validation_class";
	public static final String CF_VALIDATOR_CLASS = "default_validation_class";
	public static final String CF_COMPARATOR_CLASS = "comparator_type";
	public static final String CF_DEFAULT_KEY_CLASS = "UTF8Type";
	public static final String CF_DEFAULT_VALIDATOR_CLASS = "UTF8Type";
	public static final String CF_DEFAULT_COMPARATOR_CLASS = "UTF8Type";

	public static final Map<String, Object> COLUMN_FAMILY_OPTIONS = new HashMap<>();
	static {
		COLUMN_FAMILY_OPTIONS.put(CF_KEY_CLASS, CF_DEFAULT_KEY_CLASS);
		COLUMN_FAMILY_OPTIONS.put(CF_COMPARATOR_CLASS, CF_DEFAULT_COMPARATOR_CLASS);
		COLUMN_FAMILY_OPTIONS.put(CF_VALIDATOR_CLASS, CF_DEFAULT_VALIDATOR_CLASS);
	}
	

}
