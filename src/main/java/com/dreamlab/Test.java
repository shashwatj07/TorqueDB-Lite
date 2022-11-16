package com.dreamlab;

import com.dreamlab.constants.Constants;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.dreamlab.types.FogInfo;
import com.dreamlab.utils.Utils;
import com.google.common.geometry.S2CellId;
import com.google.protobuf.Timestamp;

import javax.swing.plaf.synth.SynthLookAndFeel;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class Test {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        Instant start = Utils.getInstantFromString("2015-02-01 01-30-00");
//        Instant end = Utils.getInstantFromString("2015-02-02 00-00-00");
//        Instant temp = Utils.getInstantFromString("2015-02-01 01-10-00");
//        List<Instant> timeChunks = Utils.getTimeChunks(TimeRange.newBuilder()
//                .setStartTimestamp(Timestamp.newBuilder().setSeconds(start.getEpochSecond()).build())
//                .setEndTimestamp(Timestamp.newBuilder().setSeconds(end.getEpochSecond()).build())
//                .build(),Constants.TIME_CHUNK_SECONDS/4);
//        System.out.println(timeChunks);
//        System.out.println(timeChunks.size());

        List<S2CellId> s2CellIds = Utils.getCellIds(-0.0000750, -0.0000750, -0.0000250, 0.0000250, Constants.S2_CELL_LEVEL);
        System.out.println(s2CellIds);
        System.out.println(s2CellIds.size());

//        FogInfo fogInfo = new FogInfo(UUID.randomUUID(), "123", 123, 0, 0, "token");
//        Utils.writeObjectToFile(List.of(fogInfo), "object.txt");
//        List<FogInfo> fogInfo1 = (List<FogInfo>) Utils.readObjectFromFile("object.txt");
//        System.out.println(fogInfo);
//        System.out.println(fogInfo1);
    }
}
