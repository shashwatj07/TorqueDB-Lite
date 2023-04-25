package com.dreamlab.types;

import com.dreamlab.edgefs.grpcServices.BlockIdReplicaMetadata;
import com.dreamlab.utils.Utils;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2Shape;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class BlockReplicaInfo implements Serializable, S2Shape {

    private static final long serialVersionUID = -8885818812042252438L;
    private final S2Polygon.Shape s2Shape;
    private final UUID blockID;
    private final List<DeviceInfo> replicaLocations;
    private final int blockIdHash;
    private final double minLat, maxLat, minLon, maxLon;
    private final Instant start;
    private final Instant stop;
    private final HashMap<String, String> metadataMap;

    public BlockReplicaInfo(UUID blockID, double minLat, double maxLat, double minLon, double maxLon, Instant start, Instant stop) {
        this.blockID = blockID;
        this.blockIdHash = blockID.hashCode();
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.minLon = minLon;
        this.maxLon = maxLon;
        this.start = start;
        this.stop = stop;
        replicaLocations = new ArrayList<>();
        metadataMap = new HashMap<>();
        S2LatLngRect s2LatLngRect = S2LatLngRect.fromPointPair(
                S2LatLng.fromDegrees(this.minLat, this.minLon),
                S2LatLng.fromDegrees(this.maxLat, this.maxLon));
        S2Loop loop = new S2Loop(Arrays.asList(s2LatLngRect.getVertex(0).toPoint(),
                s2LatLngRect.getVertex(1).toPoint(), s2LatLngRect.getVertex(2).toPoint(),
                s2LatLngRect.getVertex(3).toPoint()));
        s2Shape = new S2Polygon(loop).shape();
    }

    public BlockReplicaInfo(UUID blockID, String minLat, String maxLat, String minLon, String maxLon, Instant start, Instant stop) {
        this.blockID = blockID;
        this.blockIdHash = blockID.hashCode();
        this.minLat = Double.parseDouble(minLat);
        this.maxLat = Double.parseDouble(maxLat);
        this.minLon = Double.parseDouble(minLon);
        this.maxLon = Double.parseDouble(maxLon);
        this.start = start;
        this.stop = stop;
        replicaLocations = new ArrayList<>();
        metadataMap = new HashMap<>();
        S2LatLngRect s2LatLngRect = S2LatLngRect.fromPointPair(
                S2LatLng.fromDegrees(this.minLat, this.minLon),
                S2LatLng.fromDegrees(this.maxLat, this.maxLon));
        S2Loop loop = new S2Loop(Arrays.asList(s2LatLngRect.getVertex(0).toPoint(),
                s2LatLngRect.getVertex(1).toPoint(), s2LatLngRect.getVertex(2).toPoint(),
                s2LatLngRect.getVertex(3).toPoint()));
        s2Shape = new S2Polygon(loop).shape();
    }

    public BlockIdReplicaMetadata toMessage() {
        return BlockIdReplicaMetadata.newBuilder()
                .setBlockId(Utils.getMessageFromUUID(this.blockID))
                .addAllReplicas(replicaLocations.stream().map(DeviceInfo::toMessage).collect(Collectors.toList()))
                .build();
    }

    public void addReplicaLocation(DeviceInfo replicaLocation) {
        replicaLocations.add(replicaLocation);
    }

    public void addMetadata(String key, String value) {
        metadataMap.put(key, value);
    }

    public UUID getBlockID() {
        return blockID;
    }

    public double getMinLat() {
        return minLat;
    }

    public double getMaxLat() {
        return maxLat;
    }

    public double getMinLon() {
        return minLon;
    }

    public double getMaxLon() {
        return maxLon;
    }

    public Instant getStart() {
        return start;
    }

    public Instant getStop() {
        return stop;
    }

    public Map<String, String> getMetadataMap() {
        return Collections.unmodifiableMap(metadataMap);
    }

    public List<DeviceInfo> getReplicaLocations() {
        return Collections.unmodifiableList(replicaLocations);
    }

    @Override
    public String toString() {
        return "BlockReplicaInfo{" +
                "blockID=" + blockID +
                ", replicaLocations=" + replicaLocations +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlockReplicaInfo that = (BlockReplicaInfo) o;
        return blockID.equals(that.blockID);
    }

    @Override
    public int hashCode() {
        return blockIdHash;
    }

    @Override
    public int numEdges() {
        return s2Shape.numEdges();
    }

    @Override
    public void getEdge(int i, MutableEdge mutableEdge) {
        s2Shape.getEdge(i, mutableEdge);
    }

    @Override
    public boolean hasInterior() {
        return s2Shape.hasInterior();
    }

    @Override
    public boolean containsOrigin() {
        return s2Shape.containsOrigin();
    }

    @Override
    public int numChains() {
        return s2Shape.numChains();
    }

    @Override
    public int getChainStart(int i) {
        return s2Shape.getChainStart(i);
    }

    @Override
    public int getChainLength(int i) {
        return s2Shape.getChainLength(i);
    }

    @Override
    public void getChainEdge(int i, int i1, MutableEdge mutableEdge) {
        s2Shape.getChainEdge(i, i1, mutableEdge);
    }

    @Override
    public S2Point getChainVertex(int i, int i1) {
        return s2Shape.getChainVertex(i, i1);
    }

    @Override
    public int dimension() {
        return s2Shape.dimension();
    }
}
