package com.dreamlab;

import com.dreamlab.constants.Constants;
import com.dreamlab.constants.Keys;
import com.dreamlab.utils.Utils;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.Instant;

public class GenerateBlocks {

    private static final int INTERVAL_SECS = 30;
    private static final double INTERVAL_LAT_LON = 0.00005;
    private static double startLat = 0;
    private static double startLon = 0;
    private static long startTime = 1667000000;

    public static void main(String[] args) throws FileNotFoundException {
        String rootDir = args[0];
        int numBlocks = Integer.parseInt(args[1]);
        int numEdges = Integer.parseInt(args[2]);
        for (int edgeCount = 1; edgeCount <= numEdges; edgeCount++) {
            PrintWriter summaryWriter = new PrintWriter(String.format("%s/summary/edge_%d.csv", rootDir, edgeCount));
            PrintWriter trajectoryWriter = new PrintWriter(String.format("%s/trajectory/edge_%d.csv", rootDir, edgeCount));
            summaryWriter.println(String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s",
                    Keys.KEY_MIN_LATITUDE, Keys.KEY_MAX_LATITUDE, Keys.KEY_MIN_LONGITUDE, Keys.KEY_MAX_LONGITUDE, Keys.KEY_START_TIMESTAMP, Keys.KEY_END_TIMESTAMP, "p1", "p2", "p3"));
            int blockCount = 0;
            startLat = 0;
            startLon = 0;
            for (int p1 = 1; p1 <= 3; p1++) {
                for (int p2 = 1; p2 <= 3; p2++) {
                    for (int p3 = 1; p3 <= 3; p3++) {
                        for (int i = 0; i < numBlocks; i++) {
                            nextTime();
                            nextPos();
                            trajectoryWriter.println(String.format("%f,%f", startLat, startLon));
                            String startTimestampString = Utils.getStringFromInstant(Instant.ofEpochSecond(startTime - INTERVAL_SECS / 2));
                            String endTimestampString = Utils.getStringFromInstant(Instant.ofEpochSecond(startTime + INTERVAL_SECS / 2));
                            double minLat = startLat - INTERVAL_LAT_LON / 2;
                            double maxLat = startLat + INTERVAL_LAT_LON / 2;
                            double minLon = startLon - INTERVAL_LAT_LON / 2;
                            double maxLon = startLon + INTERVAL_LAT_LON / 2;
                            summaryWriter.println(String.format("%f,%f,%f,%f,%s,%s,%s,%s,%s",
                                    minLat, maxLat, minLon, maxLon, startTimestampString, endTimestampString, p1, p2, p3));
                            PrintWriter metadataWriter = new PrintWriter(String.format("%s/metadata/edge_%d_block_%d.json",
                                    rootDir, edgeCount, ++blockCount));
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put("p1", String.valueOf(p1))
                                    .put("p2", String.valueOf(p2))
                                    .put("p3", String.valueOf(p3))
                                    .put(Keys.KEY_START_TIMESTAMP, startTimestampString)
                                    .put(Keys.KEY_END_TIMESTAMP, endTimestampString)
                                    .put(Keys.KEY_MIN_LATITUDE, String.format("%f", minLat))
                                    .put(Keys.KEY_MAX_LATITUDE, String.format("%f", maxLat))
                                    .put(Keys.KEY_MIN_LONGITUDE, String.format("%f", minLon))
                                    .put(Keys.KEY_MAX_LONGITUDE, String.format("%f", maxLon));
                            metadataWriter.println(jsonObject.toString(4));
                            metadataWriter.close();
                        }
                    }
                }
            }
            for (int i = 0; i < 1000; i++) {
                nextPos();
                trajectoryWriter.println(String.format("%f,%f", startLat, startLon));
            }
            summaryWriter.close();
            trajectoryWriter.close();
        }
    }

    private static void nextTime() {
        startTime += INTERVAL_SECS;
    }

    private static void nextPos() {
        if (Constants.RANDOM.nextDouble() >= 0.5) {
            startLat += Math.signum(Constants.RANDOM.nextDouble() - 0.5) * INTERVAL_LAT_LON;
        }
        else {
            startLon += Math.signum(Constants.RANDOM.nextDouble() - 0.5) * INTERVAL_LAT_LON;
        }
    }
}
