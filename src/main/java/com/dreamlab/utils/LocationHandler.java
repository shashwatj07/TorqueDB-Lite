package com.dreamlab.utils;

import com.dreamlab.service.EdgeService;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LocationHandler implements Runnable {

    private final EdgeService edgeService;

    private UUID edgeId;

    private final int ttlSecs;

    private BufferedReader locationReader = null;

    private final Logger LOGGER;

    public LocationHandler(EdgeService edgeService, UUID edgeId, int ttlSecs, String trajectoryFilePath) {
        LOGGER = Logger.getLogger(String.format("[Edge: %s] ", edgeId.toString()));
        this.edgeService = edgeService;
        this.ttlSecs = ttlSecs;
        this.edgeId = edgeId;
        try {
            this.locationReader = new BufferedReader(new FileReader(trajectoryFilePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void updateLocation() throws IOException {
        String line = locationReader.readLine();
        StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
        edgeService.setLatitude(Double.parseDouble(stringTokenizer.nextToken()));
        edgeService.setLongitude(Double.parseDouble(stringTokenizer.nextToken()));
    }

    @Override
    public void run() {
        while (true) {
            try {
                updateLocation();
                Thread.sleep(1000L * ttlSecs);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, LOGGER.getName() + e.getMessage(), e);
            }
        }
    }
}
