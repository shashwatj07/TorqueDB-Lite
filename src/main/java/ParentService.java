import com.dreamlab.edgefs.grpcServices.ParentServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockRequest;
import com.dreamlab.edgefs.grpcServices.PutMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

public class ParentService extends ParentServerGrpc.ParentServerImplBase {

    private static final int TIME_CHUNK = 60 * 24;

    private final int numFogs;
    private final Map<UUID, FogPartition> fogPartitions;
    private final List<UUID> fogIds;

    public ParentService(List<FogInfo> fogDevices) {
        fogPartitions = generateFogPartitions(fogDevices);
        numFogs = fogDevices.size();
        fogIds = fogDevices.stream().map(DeviceInfo::getDeviceId).collect(Collectors.toList());
        Collections.sort(fogIds);
    }

    private Map<UUID, FogPartition> generateFogPartitions(List<FogInfo> fogDevices) {
        List<Polygon> polygons = generateVoronoiPolygons(fogDevices);
        return generateFogPolygonMap(fogDevices, polygons);
    }

    private Map<UUID, FogPartition> generateFogPolygonMap(List<FogInfo> fogDevices, List<Polygon> polygons) {
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

    private List<Polygon> generateVoronoiPolygons(List<FogInfo> fogDevices) {
        GeometryFactory geometryFactory = new GeometryFactory();

        List<Coordinate> coordinates = fogDevices.stream().map(Utils::getCoordinateFromFogInfo).collect(Collectors.toList());

        VoronoiDiagramBuilder diagramBuilder = new VoronoiDiagramBuilder();
        diagramBuilder.setSites(coordinates);
        Geometry polygonCollection = diagramBuilder.getDiagram(geometryFactory);

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

    private List<UUID> getSpatialShortlist(Polygon queryPolygon) {
        return fogPartitions.keySet().stream().filter(fogId -> fogPartitions.get(fogId).getPolygon().intersects(queryPolygon)).collect(Collectors.toList());
    }

    private List<UUID> getTemporalShortlist(TimeRange timeRange) {
        List<Instant> timeChunks = Utils.getTimeChunks(timeRange, TIME_CHUNK);
        return timeChunks.stream().map(chunk -> fogIds.get(chunk.hashCode() % numFogs)).collect(Collectors.toList());
    }

    private List<UUID> getFogsToReplicate(List<UUID> spatialShortlist, List<UUID> temporalShortlist) {
        // TODO: Hanlde collisions
        UUID spacialReplica = Utils.getRandomElement(spatialShortlist);
        UUID temporalReplica = Utils.getRandomElement(temporalShortlist);
        UUID randomReplica = Utils.getRandomElement(fogIds);
        return List.of(spacialReplica, temporalReplica, randomReplica);
    }

    @Override
    public void putBlock(PutBlockRequest request, StreamObserver<Response> responseObserver) {
        super.putBlock(request, responseObserver);
    }

    @Override
    public void putMetadata(PutMetadataRequest request, StreamObserver<Response> responseObserver) {
        super.putMetadata(request, responseObserver);
    }
}
