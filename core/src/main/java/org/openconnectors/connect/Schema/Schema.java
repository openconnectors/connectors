package org.openconnectors.connect.Schema;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

interface Schema {

    SchemaManager.SchemaType getType();

    void serialize(InputStream in, OutputStream out, Map<String, Object> parameters);

    void deserialize(InputStream in, OutputStream out, Map<String, Object> parameters);

    int getVersion();

    boolean isDeleted();

    String getSchemaInfo();

}