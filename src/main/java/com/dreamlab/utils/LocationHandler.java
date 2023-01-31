package com.dreamlab.utils;

import com.dreamlab.service.EdgeService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LocationHandler implements Runnable {

    private final EdgeService edgeService;

    private UUID edgeId;

    private final int ttlSecs;

    private List<String> trajectoryList = new ArrayList<>();

    private int trajectoryIndex;

    private final Logger LOGGER;

    public LocationHandler(EdgeService edgeService, UUID edgeId, int ttlSecs, String trajectoryFilePath) {
        LOGGER = Logger.getLogger(String.format("[Edge: %s] ", edgeId.toString()));
        this.edgeService = edgeService;
        this.ttlSecs = ttlSecs;
        this.edgeId = edgeId;
        try {
            trajectoryList = Files.readAllLines(Path.of(trajectoryFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        trajectoryIndex = 0;
    }

    private void updateLocation(int skip) {
        try {
            String line = trajectoryList.get(trajectoryIndex).strip();
            StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
            edgeService.setLatitude(Double.parseDouble(stringTokenizer.nextToken()));
            edgeService.setLongitude(Double.parseDouble(stringTokenizer.nextToken()));
            trajectoryIndex += skip + 1;
        }
        catch (Exception ex) {
            LOGGER.info("Cannot update location, exhausted trajectory information.");
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                updateLocation(59);
                Thread.sleep(1000L * ttlSecs);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, LOGGER.getName() + e.getMessage(), e);
            }
        }
    }
}
