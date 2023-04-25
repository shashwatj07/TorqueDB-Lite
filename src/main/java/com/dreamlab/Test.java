package com.dreamlab;

import com.dreamlab.constants.Constants;
import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.Point;
import com.dreamlab.types.BlockReplicaInfo;
import com.dreamlab.types.FogInfo;
import com.dreamlab.types.FogPartition;
import com.dreamlab.utils.Utils;
import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2ShapeIndex;
import com.google.common.geometry.S2ShapeIndexRegion;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class Test {

    static Map<UUID, FogPartition> fogPartitions = null;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        fogPartitions = generateFogPartitions(new ArrayList<>(Utils.readFogDetails("C:\\Users\\Shashwat\\TorqueDB-Lite\\src\\main\\resources\\fogs20.json").values()));
        String minLat = "12.8527462", maxLat = "12.8553896", minLon = "77.5872018", maxLon = "77.5887901";
        String start = "2023-01-01 00-00-00", stop = "2023-01-01 00-04-55";
        UUID blockId = UUID.fromString("269a3a65-ed10-4914-b2a7-71a7ac8f968c");
        BlockReplicaInfo blockReplicaInfo = new BlockReplicaInfo(blockId, minLat, maxLat, minLon, maxLon, Utils.getInstantFromString(start), Utils.getInstantFromString(stop));
        S2ShapeIndex s2ShapeIndex = new S2ShapeIndex();
        s2ShapeIndex.add(blockReplicaInfo);

        double minLat1 = 12.8537462, maxLat1 = 12.8573896, minLon1 = 77.5875018, maxLon1 = 77.5987901;
        BoundingBox boundingBox = BoundingBox
                .newBuilder()
                .setBottomRightLatLon(Point.newBuilder().setLatitude(minLat1).setLongitude(maxLon1).build())
                .setTopLeftLatLon(Point.newBuilder().setLatitude(maxLat1).setLongitude(minLon1).build())
                .build();
        List<S2CellId> s2CellIds = Utils.getCellIds(boundingBox, Constants.MAX_S2_CELL_LEVEL);
        List<BlockReplicaInfo> relevantBlocks = new ArrayList<>();
        S2ShapeIndexRegion s2ShapeIndexRegion = new S2ShapeIndexRegion(s2ShapeIndex);
        s2CellIds.forEach((s2CellId) -> s2ShapeIndexRegion.visitIntersectingShapes(new S2Cell(s2CellId), (s2Shape, b) -> {
            relevantBlocks.add((BlockReplicaInfo) s2Shape);
            return false;
        }));




        System.out.println(relevantBlocks);

    }

    private static List<UUID> getSpatialShortlist(BoundingBox boundingBox) {
        return getSpatialShortlist(Utils.createPolygon(boundingBox));
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
        final Polygon region = Utils.createPolygon(Constants.MIN_LAT, Constants.MAX_LAT, Constants.MIN_LON, Constants.MAX_LON);
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

    private static List<UUID> getSpatialShortlist(Polygon queryPolygon) {
        final long start = System.currentTimeMillis();
        List<UUID> spatialShortlist =  fogPartitions.keySet().stream().filter(fogId -> fogPartitions.get(fogId).getPolygon().intersects(queryPolygon)).collect(Collectors.toList());
        final long end = System.currentTimeMillis();
//        LOGGER.info(String.format("%s[Local] CoordinatorServer.getSpatialShortlist: %d", LOGGER.getName(), (end - start)));
//        LOGGER.info(String.format("%s[Count] CoordinatorServer.spatialShortlist: %d", LOGGER.getName(), spatialShortlist.size()));
        return spatialShortlist;
    }
}
