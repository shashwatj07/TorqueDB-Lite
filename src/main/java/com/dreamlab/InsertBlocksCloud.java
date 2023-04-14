package com.dreamlab;

import com.dreamlab.utils.Utils;
import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import okhttp3.OkHttpClient;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public final class InsertBlocksCloud {

    static final Logger LOGGER = Logger.getLogger("[Client] ");

    private InsertBlocksCloud() {
    }

    public static void main(String... args) {
        final String cloudInfluxDbUrl = args[0];
        final String bucket = args[1];
        final String org = args[2];
        final String token = args[3];
        final String blocksDirectory = args[4];
        final File blocksDir = new File(blocksDirectory);
        final int interval = Integer.parseInt(args[5]);
        final List<String> blocks = Arrays.stream(blocksDir.listFiles()).map(File::getAbsolutePath).collect(Collectors.toList());
        Collections.sort(blocks);
        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                .writeTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                .readTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                .retryOnConnectionFailure(true);
        InfluxDBClientOptions influxDBClientOptions = InfluxDBClientOptions.builder()
                .authenticateToken(token.toCharArray())
                .org(org)
                .connectionString(cloudInfluxDbUrl) // ?readTimeout=1m&connectTimeout=1m&writeTimeout=1m
                .okHttpClient(okHttpClient)
                .logLevel(LogLevel.BASIC)
                .bucket(bucket)
                .build();
        final InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxDBClientOptions);
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
        for (int i = 0; i < blocks.size(); i++) {
            String blockFilePath = blocks.get(i);
            String blockId = new File(blockFilePath).getName().substring(5, 41);
            try {
                LOGGER.info("Inserting: " + blockId);
                final long start = System.currentTimeMillis();
                writeApi.writeRecord(WritePrecision.MS, Utils.getBytes(blockFilePath).toStringUtf8());
                final long end = System.currentTimeMillis();
                LOGGER.info(String.format("[Client] [Outer %s] EdgeServer.putBlockAndMetadata: %d", blockId, (end - start)));
                LOGGER.info("Success: " + blockId);
                final long sleepTime = interval * 1000L - (end - start);
                Thread.sleep(sleepTime >= 0? sleepTime : 0);
            }
            catch (Exception ex) {
                LOGGER.info("Failed to insert " + blockId);
                ex.printStackTrace();
            }
        }
        influxDBClient.close();
    }
}
