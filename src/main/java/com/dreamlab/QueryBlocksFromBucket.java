package com.dreamlab;

import com.dreamlab.constants.Constants;
import com.dreamlab.query.InfluxDBQuery;
import com.dreamlab.utils.Utils;
import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;
import okhttp3.OkHttpClient;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class QueryBlocksFromBucket {
    static final Logger LOGGER = Logger.getLogger("[Client] ");

    private QueryBlocksFromBucket() {
    }

    public static void main(String... args) {
        final String bucket = args[0];
        final String token = args[1];
        final String queryFilePath = args[2];
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
        QueryApi queryApi = influxDBClient.getQueryApi();
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(Files.readString(Paths.get(queryFilePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(String workload : jsonObject.keySet()) {
            JSONObject queries = (JSONObject) jsonObject.get(workload);
            for (String queryId : queries.keySet()) {
                JSONObject params = (JSONObject) queries.get(queryId);
                InfluxDBQuery influxDBQuery = new InfluxDBQuery();
                influxDBQuery.addQueryId();
                influxDBQuery.addRegion(params.getString("minLat"), params.getString("maxLat"), params.getString("minLon"), params.getString("maxLon"));
                influxDBQuery.addFilter("pollution", List.of(), List.of());
                String start = params.getString("start");
                String end = params.getString("stop");
                Instant startInstant = Utils.getInstantFromString(start);
                Instant endInstant = Utils.getInstantFromString(end);
//                startInstant = startInstant.minus(330, ChronoUnit.MINUTES);
//                endInstant = endInstant.minus(330, ChronoUnit.MINUTES);
                influxDBQuery.addRange(Utils.getStringFromInstant(startInstant), Utils.getStringFromInstant(endInstant));
                influxDBQuery.addBucketName(bucket);
                influxDBQuery.addKeep(Arrays.asList("_value", "_time"));
                System.out.println(getFluxQuery(influxDBQuery));
                try {
                    long startTime = System.currentTimeMillis();
                    queryApi.queryRaw(getFluxQuery(influxDBQuery));
                    long endTime = System.currentTimeMillis();
                    System.out.println(queryId + " " + (endTime-startTime));
                }
                catch (Exception ex) {
                    System.out.println(queryId + " Failed");
                }
            }
        }
    }
    public static String getFluxQuery(InfluxDBQuery influxDBQuery) {
        StringBuilder query = new StringBuilder();
        query.append("import \"experimental/geo\" ");
        query.append("from(bucket:\"").append(influxDBQuery.getBucket()).append("\")");

        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;

        LocalDateTime start = LocalDateTime.parse(
                influxDBQuery.getOperations().get("range").get("start"),
                DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN)).minusMinutes(330);
        LocalDateTime stop = LocalDateTime.parse(
                influxDBQuery.getOperations().get("range").get("stop"),
                DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN)).minusMinutes(330);

        LocalDateTime temp_date = stop.minusSeconds(1);

        query.append("|> range(start:").append(start.format(formatter)).append("Z,stop:")
                .append(temp_date.format(formatter)).append("Z)");


        HashMap<String, String> region = influxDBQuery.getOperations().get("region");
        query.append("|> geo.filterRows(region: ")
                .append("{ minLat: ").append(region.get("minLat"))
                .append(", maxLat: ").append(region.get("maxLat"))
                .append(", minLon: ").append(region.get("minLon"))
                .append(", maxLon: ").append(region.get("maxLon"))
                .append(" }, strict: true)");

        query.append("|>keep(columns:[");
        Map<String, String> keepMap = influxDBQuery.getOperations().get("keep");
        for (String k : keepMap.keySet()) {
            query.append("\"").append(keepMap.get(k)).append("\"").append(",");
        }
        query = new StringBuilder(query.substring(0, query.length() - 1));
        query.append("])");

        return query.toString();
    }
}
