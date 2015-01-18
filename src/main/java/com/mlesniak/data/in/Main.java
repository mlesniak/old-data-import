package com.mlesniak.data.in;

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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
            hdfsWrite();
        } catch (Exception e) {
            log.error("Exception: {}", e.getMessage());
        }
    }

    private void hdfsWrite() throws IOException, URISyntaxException {
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

        log.info("Creating file");
        OutputStream os = hdfs.create(file);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        br.write(content);
        br.close();
        hdfs.close();
        log.info("Bye");
    }
}