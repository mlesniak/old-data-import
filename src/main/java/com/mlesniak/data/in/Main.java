package com.mlesniak.data.in;

import com.mlesniak.data.in.schema.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.io.IOException;

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
        String hdfsServer = env.getProperty("hdfsServer");
        String fileName = env.getProperty("fileName");

        Configuration configuration = new Configuration();
        log.info("Connection to {}", hdfsServer);
        Path outputPath = new Path(hdfsServer + "/" + fileName);

        // Add timestamp for uniqueness (since we use the file in a warehouse context).
        outputPath = outputPath.suffix("-" + System.currentTimeMillis());
        log.info("Filename {}", outputPath.toUri().toString());

        DataAvroParquetWriter<Table> writer = new DataAvroParquetWriter<>(outputPath, Table.getClassSchema());
        log.info("Writing objects.");
        writeObjects(writer);
        log.info("Done with writing.");
        writer.close();
        log.info("Done with closing (actual writing).");
    }

    private void writeObjects(DataAvroParquetWriter<Table> writer) throws IOException {
        int count = Integer.parseInt(env.getProperty("count"));
        for (int i = 0; i < count; i++) {
            Table table = Table.newBuilder()
                    .setColumn0(randInt())
                    .setColumn1(randInt())
                    .setColumn2(randInt())
                    .setColumn3(randInt())
                    .setColumn4(randInt())
                    .setColumn5(randInt())
                    .setColumn6(randInt())
                    .setColumn7(randInt())
                    .setColumn8(randInt())
                    .setColumn9(randInt())
                    .build();
            writer.write(table);
        }
    }

    private Integer randInt() {
        return (int)(Math.random() * Integer.MAX_VALUE);
    }
}