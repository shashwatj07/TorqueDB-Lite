package com.dreamlab;

import com.dreamlab.constants.Constants;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class QueryLocal {
    public static void main(String[] args) throws IOException {
        char token[] = "mfn78cfytLM_Ii4smj_toGHCzZlB6DgT42idgHeY1G9VsUwbBv45viUXK5lPEtS9xiOS9GzjR_hpPhRRC07arA==".toCharArray();
        InfluxDBClient influxDBClient = InfluxDBClientFactory
                .create(InfluxDBClientOptions.builder()
                        .bucket("bucket")
                        .org("org")
                        .authenticateToken(token)
                        .connectionString("http://localhost:8086")
                        .build());
        QueryApi queryApi = influxDBClient.getQueryApi();

        final File blocksDir = new File("data/blocks/0000");
        final List<String> blocks = Arrays.stream(blocksDir.listFiles()).map(File::getAbsolutePath).collect(Collectors.toList());
        Collections.sort(blocks);

        PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter("target/output.csv", true)));

        for (int k = 0; k < 60; k++) {
            System.out.println(k);

            StringBuilder orType = new StringBuilder("from(bucket: \"bucket\")\n" +
                    "|> range(start:2022-12-31T18:30:00Z,stop:2023-01-02T18:30:00Z)\n" +
                    "");
//                "|> filter(fn: (r) => r._field==\"aqi\" and r._value<1000000)\n");
            StringBuilder regexType = new StringBuilder("from(bucket: \"bucket\")\n" +
                    "|> range(start:2022-12-31T18:30:00Z,stop:2023-01-02T18:30:00Z)\n" +
                    "");
//                "|> filter(fn: (r) => r._field==\"aqi\" and r._value<1000000)\n");
            String firstBlockId = new File(blocks.get(0)).getName().substring(5, 41);
            orType.append(String.format("|> filter(fn: (r)=> r.blockid==\"%s\" ", firstBlockId));
            regexType.append(String.format("|> filter(fn: (r)=> r.blockid =~ /%s", firstBlockId));
            int j = Constants.RANDOM.nextInt(160) + 1;
            System.out.println(j);
            for (int i = 0; i < j; i++) {
                String blockFilePath = blocks.get(i);
                String blockId = new File(blockFilePath).getName().substring(5, 41);
                orType.append(String.format("or r.blockid==\"%s\" ", blockId));
                regexType.append(String.format("|%s", blockId));
            }
            orType.append(")");
            regexType.append("/ )");
            printWriter.append(j + ",");
            long start = System.currentTimeMillis();
            queryApi.queryRaw(regexType.toString());
            long end = System.currentTimeMillis();
            printWriter.append((end - start) + ",");
            if (j > 160) {
                printWriter.append("-").append('\n');
            }
            else {
                start = System.currentTimeMillis();
                queryApi.queryRaw(orType.toString());
                end = System.currentTimeMillis();
                printWriter.append((end - start)+"").append('\n');
            }

        }
        printWriter.close();
    }
}
