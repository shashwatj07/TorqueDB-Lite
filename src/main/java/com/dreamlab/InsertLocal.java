package com.dreamlab;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class InsertLocal {
    public static void main(String[] args) throws IOException {
        char token[] = "mfn78cfytLM_Ii4smj_toGHCzZlB6DgT42idgHeY1G9VsUwbBv45viUXK5lPEtS9xiOS9GzjR_hpPhRRC07arA==".toCharArray();
        InfluxDBClient influxDBClient = InfluxDBClientFactory
                .create(InfluxDBClientOptions.builder()
                        .bucket("bucket")
                        .org("org")
                        .authenticateToken(token)
                        .connectionString("http://localhost:8086")
                        .build());
        WriteApiBlocking writeApiBlocking = influxDBClient.getWriteApiBlocking();

        final File blocksDir = new File("data/blocks/0000");
        final List<String> blocks = Arrays.stream(blocksDir.listFiles()).map(File::getAbsolutePath).collect(Collectors.toList());
        Collections.sort(blocks);

        for (int i = 0; i < blocks.size(); i++) {
            String blockFilePath = blocks.get(i);
            String blockId = new File(blockFilePath).getName().substring(5, 41);

            String lines = Files.readString(Path.of(blockFilePath), Charset.defaultCharset());

            writeApiBlocking.writeRecord(WritePrecision.MS, lines);
            System.out.println(i);
        }
    }
}
