package org.openconnectors.aerospike;

final class AeroSpikeConfig {
    static final class Keys {
        static final String AEROSPIKE_SEEDHOSTS = "aerospike.seedhosts";
        static final String AEROSPIKE_KEYSPACE = "aerospike.keyspace";
        static final String AEROSPIKE_COLUMNNAME = "aerospike.columnname";
        static final String AEROSPIKE_USERNAME = "aerospike.username";
        static final String AEROSPIKE_PASSWORD = "aerospike.password";
        static final String AEROSPIKE_KEYSET = "aerospike.keyset";
        static final String AEROSPIKE_WRITETIMEOUTMS = "aerospike.writetimeoutms";
        static final String AEROSPIKE_MAXRETRIES = "aerospike.maxretries";
    }

    static final class Defaults {
        static final int MAX_RETRIES = 1;
        static final int WRITE_TIMEOUT_MS = 100;
        static final String KEY_SET = "";
    }

    static final String AEROSPIKE_CONNECTOR_VERSION = "0.0.1";
}
