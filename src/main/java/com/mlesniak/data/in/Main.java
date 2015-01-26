package com.mlesniak.data.in;

import com.mlesniak.data.in.schema.Table;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    public static final int COLUMNS = 10;

    @Autowired
    private Environment env;

    @Value("${maxValue}")
    private int maxValue;

    private int[] maxValues = new int[COLUMNS];

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Application started");
        try {
            determineMaxColumns();
            int threads = Integer.parseInt(env.getProperty("threads"));
            int tasks = Integer.parseInt(env.getProperty("tasks"));
            LOG.info("Multithreading threads={}, tasks={}", threads, tasks);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            for (int i = 0; i < tasks; i++) {
                pool.submit(() -> {
                    try {
                        LOG.info("Running thread {}", Thread.currentThread().getId());
                        writeParquetFile();
                    } catch (Exception e) {
                        LOG.error("Error running thread: {}", e.getMessage());
                    }
                });
            }
            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            stopWatch.stop();
            long seconds = stopWatch.getTime() / 1000;
            LOG.info("Writing took {} seconds", seconds);
        } catch (Exception e) {
            LOG.error("Exception: {}", e.getMessage());
        }
        LOG.info("Application finished");
    }

    private void determineMaxColumns() {
        for (int i = 0; i < COLUMNS; ++i) {
            String propValue = env.getProperty("maxValue." + i);
            if (StringUtils.isEmpty(propValue)) {
                maxValues[i] = maxValue;
            } else {
                maxValues[i] = Integer.parseInt(propValue);
            }
        }
    }

    private void writeParquetFile() throws Exception {
        String hdfsServer = env.getProperty("hdfsServer");
        String fileName = env.getProperty("fileName");

        LOG.info("Connection to {}", hdfsServer);
        Path outputPath = new Path(hdfsServer + "/" + fileName);
        long id = Thread.currentThread().getId();

        // Add timestamp for uniqueness (since we use the file in a warehouse context).
        outputPath = outputPath.suffix("-" + System.currentTimeMillis()).suffix("-" + id);
        LOG.info("Filename {}", outputPath.toUri().toString());

        // Show impala statement which should be used to create the external table.
        String impala = String.format("create external table %1s like parquet '%2s' stored as parquet location '%3s';",
                env.getProperty("tableName"),
                outputPath.toUri().getPath(),
                env.getProperty("warehouse"));
        LOG.info(impala);

        DataAvroParquetWriter<Table> writer = new DataAvroParquetWriter<>(outputPath, Table.getClassSchema());
        LOG.info("Writing objects.");
        writeObjects(writer);
        LOG.info("Done with writing.");
        writer.close();
        LOG.info("Done with closing (actual writing).");
    }

    private void writeObjects(DataAvroParquetWriter<Table> writer) throws IOException {
        int count = Integer.parseInt(env.getProperty("count"));
        for (int i = 0; i < count; i++) {
            Table table = Table.newBuilder()
                    .setColumn0(randInt(0))
                    .setColumn1(randInt(1))
                    .setColumn2(randInt(2))
                    .setColumn3(randInt(3))
                    .setColumn4(randInt(4))
                    .setColumn5(randInt(5))
                    .setColumn6(randInt(6))
                    .setColumn7(randInt(7))
                    .setColumn8(randInt(8))
                    .setColumn9(randInt(9))
                    .build();
            writer.write(table);
        }
    }

    private Integer randInt(int column) {
        return (int) (Math.random() * maxValues[column]);
    }
}
