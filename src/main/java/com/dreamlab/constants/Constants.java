package com.dreamlab.constants;

import com.dreamlab.types.BlockReplicaInfo;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public final class Constants {
    public static final int TIME_CHUNK_SECONDS = 120;
    public static final SecureRandom RANDOM = new SecureRandom();
    public static final int S2_CELL_LEVEL = 20;
    public static final double MIN_LAT = 12.834;
    public static final double MAX_LAT = 13.144;
    public static final double MIN_LON = 77.460;
    public static final double MAX_LON = 77.784;
    public static final String DATE_TIME_PATTERN = "uuuu-MM-dd HH-mm-ss";
    public static final String ZONE_ID = "Z";
    public static final int N_THREADS = 16;
    public static final ConcurrentLinkedQueue<BlockReplicaInfo> EMPTY_LIST_REPLICA = new ConcurrentLinkedQueue<>();
    public static final ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>> EMPTY_MAP_STRING_LIST_REPLICA = new ConcurrentHashMap<>();
    public static XXHash64 XXHASH64 = XXHashFactory.fastestJavaInstance().hash64();
    public static final int SEED_HASH = 0;
}
