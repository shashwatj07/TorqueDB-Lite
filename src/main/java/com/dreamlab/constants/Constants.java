package com.dreamlab.constants;

import com.dreamlab.types.BlockReplicaInfo;

import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public final class Constants {
    public static final int TIME_CHUNK_SECONDS = 120;
    public static final SecureRandom RANDOM = new SecureRandom();
    public static final int S2_CELL_LEVEL = 20;
    public static final String DATE_TIME_PATTERN = "uuuu-MM-dd HH-mm-ss";
    public static final String ZONE_ID = "Z";

    public static final int N_THREADS = 16;

    public static final ConcurrentLinkedQueue<BlockReplicaInfo> EMPTY_LIST_REPLICA = new ConcurrentLinkedQueue<>();

    public static final ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>> EMPTY_MAP_STRING_LIST_REPLICA = new ConcurrentHashMap<>();
}
