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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public final class InsertBlocksIntoBucket {

    static final Logger LOGGER = Logger.getLogger("[Client] ");

    private InsertBlocksIntoBucket() {
    }

    public static void main(String... args) {
        final String bucket = args[0];
        final String token = args[1];
        final String blocksDirectory = args[2];
        LOGGER.info("Inserting blocks to " + bucket);
        final File blocksDir = new File(blocksDirectory);
        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                .writeTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                .readTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                .retryOnConnectionFailure(true);
        InfluxDBClientOptions influxDBClientOptions = InfluxDBClientOptions.builder()
                .authenticateToken(token.toCharArray())
                .org("org")
                .connectionString("http://localhost:8086?readTimeout=60m&connectTimeout=60m&writeTimeout=60m") // ?readTimeout=1m&connectTimeout=1m&writeTimeout=1m
                .okHttpClient(okHttpClient)
                .logLevel(LogLevel.BASIC)
                .bucket("bucket")
                .build();
        final InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxDBClientOptions);
        final List<File> blockDirs = Arrays.stream(blocksDir.listFiles()).collect(Collectors.toList());
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        for (int i = 0; i < blockDirs.size(); i++) {
            File edgeBlockDir = blockDirs.get(i);
            final List<String> blocks = Arrays.stream(edgeBlockDir.listFiles()).map(File::getAbsolutePath).collect(Collectors.toList());
            for (String block : blocks) {
                try {
                    final long start = System.currentTimeMillis();
                    writeApi.writeRecord(WritePrecision.MS, Utils.getBytes(block).toStringUtf8());
                    final long end = System.currentTimeMillis();
                    System.out.println(end - start);
                }
                catch (Exception ex) {
                    LOGGER.info("Failed to insert block");
                    ex.printStackTrace();
                }
            }
        }
    }
}
