package com.mlesniak.data.in;

import com.mlesniak.data.in.schema.Table;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private Logger log = LoggerFactory.getLogger(Main.class);

    @Autowired
    private Environment env;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        try {
            OutputStream outputStream = getHDFSOutputStream();
            writeAvroFile(outputStream);
            outputStream.close();
            log.info("Done.");
        } catch (Exception e) {
            log.error("Exception: {}", e.getMessage());
        }
    }

    private void writeAvroFile(OutputStream out) throws IOException {
        log.info("Creating object");
        Table table = new Table();
        table.setColumn0("value-0");
        table.setColumn1("value-1");
        log.info("Object: {}", table.toString());

        log.info("Writing to stream");
        DatumWriter<Table> tableDatumWriter = new SpecificDatumWriter<>(Table.class);
        DataFileWriter<Table> fileWriter = new DataFileWriter<>(tableDatumWriter);
        fileWriter.create(table.getSchema(), out);
        fileWriter.append(table);
        fileWriter.close();
    }

    private OutputStream getHDFSOutputStream() throws IOException, URISyntaxException {
        log.info("Application started");

        String hdfsServer = env.getProperty("hdfsServer");
        String fileName = env.getProperty("fileName");
        String content = env.getProperty("content");

        Configuration configuration = new Configuration();
        log.info("Connection to {}", hdfsServer);
        FileSystem hdfs = FileSystem.get(new URI(hdfsServer), configuration);
        Path file = new Path(hdfsServer + "/" + fileName);

        // Remove file if already existing (for testing purposes).
        log.info("Filename: {}", fileName);
        if (hdfs.exists(file)) {
            log.info("Removing file");
            hdfs.delete(file, true);
        }

        log.info("Creating stream for file");
        return hdfs.create(file);
    }
}