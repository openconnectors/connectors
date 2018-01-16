package org.openconnectors.connect.Schema;

public interface SchemaManager {

    enum SchemaType{
        Avro,
        Protobuf,
        Thrift,
        Json
    }

    Schema getSchema(String schemaId);

    // Get latest schema based on compatibility to a known one
    Schema getLatestCompatibleSchema(String schemaId, Schema compatibleTarget);

    // Get Schema by specific version id
    Schema getSchema(String schemaId, int version);
}
