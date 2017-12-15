package org.openconnectors.cassandra;

public final class CassandraConfig {
    static final class Keys {
        static final String CASSANDRA_ROOTS = "cassandra.roots";
        static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
        static final String CASSANDRA_KEYNAME = "cassandra.keyname";
        static final String CASSANDRA_COLUMNFAMILY = "cassandra.columnfamily";
        static final String CASSANDRA_COLUMNNAME = "cassandra.columnname";
    }

    static final String CASSANDRA_CONNECTOR_VERSION = "0.0.1";
}
