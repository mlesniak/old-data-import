package com.mlesniak.data.in;

import com.mlesniak.data.in.schema.Table;
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

import java.net.URI;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private Logger log = LoggerFactory.getLogger(Main.class);

    @Autowired
    private Environment env;

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Application started");
        try {
            writeParquetFile();
        } catch (Exception e) {
            log.error("Exception: {}", e.getMessage());
        }
        log.info("Application finished");
    }

    private void writeParquetFile() throws Exception {
        log.info("Creating objects");
        Table table1 = new Table();
        table1.setColumn0("value-0");
        table1.setColumn1("value-1");
        log.info("Table 1: {}", table1.toString());

        Table table2 = new Table();
        table2.setColumn0("t2-value-0");
        table2.setColumn1("t2-value-1");
        table2.setColumn2("t2-value-2");
        log.info("Table 2: {}", table2.toString());

        String hdfsServer = env.getProperty("hdfsServer");
        String fileName = env.getProperty("fileName");

        Configuration configuration = new Configuration();
        log.info("Connection to {}", hdfsServer);
        FileSystem hdfs = FileSystem.get(new URI(hdfsServer), configuration);
        Path outputPath = new Path(hdfsServer + "/" + fileName);

        // Remove file if already existing (for testing purposes).
        log.info("Filename: {}", fileName);
        if (hdfs.exists(outputPath)) {
            log.info("Removing file");
            hdfs.delete(outputPath, true);
        }

        DataAvroParquetWriter<Table> writer = new DataAvroParquetWriter<>(outputPath, table1.getSchema());
        log.info("Writing objects.");
        writer.write(table1);
        writer.write(table2);
        writer.close();
    }
}