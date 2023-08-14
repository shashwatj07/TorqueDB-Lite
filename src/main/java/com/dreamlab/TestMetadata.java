package com.dreamlab;

import com.dreamlab.constants.Constants;
import com.dreamlab.constants.Keys;
import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.Point;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.dreamlab.types.FogInfo;
import com.dreamlab.types.FogPartition;
import com.dreamlab.utils.Utils;
import org.json.JSONObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class TestMetadata {

    static Map<UUID, FogInfo> fogDetails;

    static List<UUID> fogIds;

    static int numFogs;

    static Map<UUID, FogPartition> fogPartitions;

    public static void main(String[] args) throws IOException {
        String rootDirectory = "data/data3/metadata-400"; // Replace this with the actual path to your root directory
        final String fogsConfigFilePath = "data/data3/fogs80.json";
        fogDetails = Utils.readFogDetails(fogsConfigFilePath);
        fogPartitions = generateFogPartitions(new ArrayList<>(fogDetails.values()));
        fogIds = new ArrayList<>(fogDetails.keySet());
        Collections.sort(fogIds);
        numFogs = fogIds.size();
        readAndProcessJSONFiles(rootDirectory);
    }

    public static void readAndProcessJSONFiles(String rootDirectory) throws IOException {
        File rootDir = new File(rootDirectory);
        File[] subDirectories = rootDir.listFiles(File::isDirectory);
        FileWriter csvWriter = new FileWriter("data/data3/replica-400.csv");
        csvWriter.append("block,random,spatial,temporal\n");
        if (subDirectories == null) {
            System.out.println("No subdirectories found in the root directory.");
            return;
        }

        for (File subDir : subDirectories) {
            File[] jsonFiles = subDir.listFiles((dir, name) -> name.endsWith(".json"));

            if (jsonFiles == null) {
                System.out.println("No JSON files found in directory: " + subDir.getAbsolutePath());
                continue;
            }

            for (File jsonFile : jsonFiles) {
                try {
                    String jsonContent = Files.readString(Paths.get(jsonFile.getAbsolutePath()));
                    JSONObject jsonObject = new JSONObject(jsonContent);
                    System.out.println("JSON Object for file " + jsonFile.getName() + ":");
//                    System.out.println(jsonObject.toString());
                    UUID blockId = UUID.fromString(jsonObject.getString("blockid"));
                    Instant startInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_START_TIMESTAMP));
                    Instant endInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_END_TIMESTAMP));
                    double minLatitude = jsonObject.getDouble(Keys.KEY_MIN_LATITUDE);
                    double minLongitude = jsonObject.getDouble(Keys.KEY_MIN_LONGITUDE);
                    double maxLatitude = jsonObject.getDouble(Keys.KEY_MAX_LATITUDE);
                    double maxLongitude = jsonObject.getDouble(Keys.KEY_MAX_LONGITUDE);
                    Polygon boundingBoxPolygon = boundingBoxPolygon = Utils.createPolygon(minLatitude, maxLatitude, minLongitude, maxLongitude);
                    final TimeRange.Builder timeRangeBuilder = TimeRange.newBuilder();
                    final BoundingBox.Builder boundingBoxBuilder = BoundingBox.newBuilder();
                    timeRangeBuilder
                            .setStartTimestamp(Utils.getTimestampMessageFromInstant(startInstant))
                            .setEndTimestamp(Utils.getTimestampMessageFromInstant(endInstant));
                    boundingBoxBuilder
                            .setTopLeftLatLon(Point.newBuilder()
                                    .setLatitude(maxLatitude)
                                    .setLongitude(minLongitude)
                                    .build())
                            .setBottomRightLatLon(Point.newBuilder()
                                    .setLatitude(minLatitude)
                                    .setLongitude(maxLongitude)
                                    .build());

                    TimeRange timeRange = timeRangeBuilder.build();
    //        BoundingBox boundingBox = boundingBoxBuilder.build();
                    List<UUID> spatialShortlist = getSpatialShortlist(boundingBoxPolygon, blockId);
                    List<UUID> temporalShortlist = getTemporalShortlist(timeRange, blockId);
                    UUID randomReplica = getFogHashByBlockId(blockId);
                    UUID temporalReplica = getFogHashByTimeRange(startInstant, endInstant);
                    UUID spatialReplica = getFogHashByBoundingBox(boundingBoxPolygon);
                    List<UUID> blockReplicaFogIds = getFogsToReplicate(blockId, spatialShortlist, temporalShortlist, randomReplica, temporalReplica, spatialReplica);
                    StringBuilder row = new StringBuilder();
                    row.append(blockId).append(",");
                    for (UUID uuid : blockReplicaFogIds) {
                        row.append(uuid.toString()).append(",");
                    }
                    row.deleteCharAt(row.length() - 1); // Remove the trailing comma
                    csvWriter.append(row.toString()).append("\n");

            } catch (IOException e) {
                    System.err.println("Error reading JSON file: " + jsonFile.getAbsolutePath());
                    e.printStackTrace();
                }
            }
        }
        csvWriter.close();
    }

    private static List<UUID> getSpatialShortlist(Polygon queryPolygon, UUID queryId) {
        final long start = System.currentTimeMillis();
        List<UUID> spatialShortlist =  fogPartitions.keySet().stream().filter(fogId -> fogPartitions.get(fogId).getPolygon().intersects(queryPolygon)).collect(Collectors.toList());
        final long end = System.currentTimeMillis();
        return spatialShortlist;
    }

    private static List<UUID> getTemporalShortlist(TimeRange timeRange, UUID queryId) {
        final long start = System.currentTimeMillis();
        List<Instant> timeChunks = Utils.getTimeChunks(timeRange, Constants.TIME_CHUNK_SECONDS);
        List<UUID> temporalShortlist = timeChunks.stream().map(chunk -> fogIds.get((int) Math.abs(Constants.XXHASH64.hash(Utils.serializeObject(chunk), Constants.SEED_HASH) % numFogs))).collect(Collectors.toList());
        final long end = System.currentTimeMillis();
        return temporalShortlist;
    }

    private static List<UUID> getFogsToReplicate(UUID blockId, List<UUID> spatialShortlist, List<UUID> temporalShortlist,
                                         UUID randomReplica, UUID temporalReplica, UUID spatialReplica) {
        List<UUID> replicas = new ArrayList<>(List.of(randomReplica));
        if (replicas.contains(spatialReplica)) {
            boolean added = false;
            for (UUID spatialReplicaCandidate : spatialShortlist) {
                if (!replicas.contains(spatialReplicaCandidate)) {
                    replicas.add(spatialReplicaCandidate);
                    spatialReplica = spatialReplicaCandidate;
                    added = true;
                    break;
                }
            }
            if (!added) {
                for (UUID replicaCandidate : fogIds) {
                    replicas.add(replicaCandidate);
                    spatialReplica = replicaCandidate;
                    break;
                }
            }
        }
        else {
            replicas.add(spatialReplica);
        }
        if (replicas.contains(temporalReplica)) {
            boolean added = false;
            for (UUID temporalReplicaCandidate : temporalShortlist) {
                if (!replicas.contains(temporalReplicaCandidate)) {
                    replicas.add(temporalReplicaCandidate);
                    temporalReplica = temporalReplicaCandidate;
                    added = true;
                    break;
                }
            }
            if (!added) {
                for (UUID replicaCandidate : fogIds) {
                    replicas.add(replicaCandidate);
                    temporalReplica = replicaCandidate;
                    break;
                }
            }
        }
        else {
            replicas.add(temporalReplica);
        }
//        o: for (UUID spatialReplicaCandidate : spatialShortlist) {
//            for (UUID temporalReplicaCandidate : temporalShortlist) {
//                if (!spatialReplicaCandidate.equals(temporalReplicaCandidate)
//                        && !replicas.contains(spatialReplicaCandidate)
//                        && !replicas.contains(temporalReplicaCandidate)) {
//                    replicas.add(spatialReplicaCandidate);
//                    replicas.add(temporalReplicaCandidate);
//                    break o;
//                }
//            }
//        }
//        if (replicas.size() < 3) {
//            for (UUID replicaCandidate : fogIds) {
//                replicas.add(replicaCandidate);
//                if (replicas.size() >= 3) {
//                    break;
//                }
//            }
//        }
        return replicas;
    }

    private static UUID getFogHashByBlockId(UUID blockId) {
        return fogIds.get((int) Math.abs(Constants.XXHASH64.hash(Utils.serializeObject(blockId.hashCode()), Constants.SEED_HASH) % numFogs));
    }

    private static UUID getFogHashByBoundingBox(Polygon boundingBox) {
        org.locationtech.jts.geom.Point centroid = boundingBox.getCentroid();
        for (Map.Entry<UUID, FogPartition> entry : fogPartitions.entrySet()) {
            if (entry.getValue().getPolygon().intersects(centroid)) {
                return entry.getKey();
            }
        }
        return fogIds.get(0);
    }

    private static UUID getFogHashByTimeRange(Instant startInstant, Instant endInstant) {
        Duration duration = Duration.between(startInstant, endInstant);
        Instant midInstant = startInstant.plus(duration.toMillis() / 2, ChronoUnit.MILLIS);
        Instant midChunk = Instant.ofEpochSecond(midInstant.getEpochSecond() - ((midInstant.getEpochSecond() - Instant.MIN.getEpochSecond()) % Constants.TIME_CHUNK_SECONDS));
        return fogIds.get((int) Math.abs(Constants.XXHASH64.hash(Utils.serializeObject(midChunk), Constants.SEED_HASH) % numFogs));
    }

    private static Map<UUID, FogPartition> generateFogPartitions(List<FogInfo> fogDevices) {
        List<Polygon> polygons = generateVoronoiPolygons(fogDevices);
        return generateFogPolygonMap(fogDevices, polygons);
    }

    private static Map<UUID, FogPartition> generateFogPolygonMap(List<FogInfo> fogDevices, List<Polygon> polygons) {
        Map<UUID, FogPartition> fogPartitionMap = new HashMap<>();
        for (FogInfo fog : fogDevices) {
            for (Polygon polygon : polygons) {
                Coordinate coordinate = new Coordinate(fog.getLongitude(), fog.getLatitude());
                if (polygon.contains(GeometryFactory.createPointFromInternalCoord(coordinate, polygon))) {
                    fogPartitionMap.put(fog.getDeviceId(), Utils.getFogPartition(fog, polygon));
                }
            }
        }
        return fogPartitionMap;
    }

    private static List<Polygon> generateVoronoiPolygons(List<FogInfo> fogDevices) {
        // 12.644, 13.334, 77.262, 77.982 - 80 fogs
        // 12.834, 13.1437, 77.4601, 77.784 - 20 fogs
        // 13.04, 12.91, 77.52, 77.69
        final Polygon region = Utils.createPolygon(12.91, 13.04, 77.52, 77.69);
        List<Coordinate> coordinates = fogDevices.stream().map(Utils::getCoordinateFromFogInfo).collect(Collectors.toList());
        VoronoiDiagramBuilder diagramBuilder = new VoronoiDiagramBuilder();
        diagramBuilder.setSites(coordinates);
        diagramBuilder.setClipEnvelope(region.getEnvelopeInternal());
        Geometry polygonCollection = diagramBuilder.getDiagram(region.getFactory());

        List<Polygon> voronoiPolygons = new ArrayList<>();

        if (polygonCollection instanceof GeometryCollection) {
            GeometryCollection geometryCollection = (GeometryCollection) polygonCollection;
            for (int polygonIndex = 0; polygonIndex < geometryCollection.getNumGeometries(); polygonIndex++) {
                Polygon polygon = (Polygon) geometryCollection.getGeometryN(polygonIndex);
                voronoiPolygons.add(polygon);
            }
        }
        return voronoiPolygons;
    }
}

