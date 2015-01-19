package com.mlesniak.data.in;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroParquetWriter;

import java.io.IOException;
import java.util.logging.Level;

/**
 * Writer implementation which prevents parquet logging bugs (in combination with StaticLoggerBinder).
 *
 * @author Michael Lesniak (mail@mlesniak.com)
 */
public class DataAvroParquetWriter<T  extends IndexedRecord> extends AvroParquetWriter<T> {
    public DataAvroParquetWriter(Path file, Schema avroSchema) throws IOException {
        super(file, avroSchema);
        java.util.logging.Logger.getLogger("parquet.hadoop").setLevel(Level.SEVERE);
    }
}
