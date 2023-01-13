package com.dreamlab.utils;

import com.dreamlab.api.Condition;
import com.dreamlab.api.TSDBQuery;
import com.dreamlab.constants.Constants;
import com.dreamlab.constants.Model;
import com.dreamlab.query.FieldCondition;
import com.dreamlab.query.TagCondition;
import com.dreamlab.types.CostModelOutput;
import com.dreamlab.types.ExecPlan;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

public class QueryDecomposition {

    public int shortCirtuit = 0;

    public int aggwin_shortCircuit = 0;

    int DELTA = 24; // DELTA is calculated from midnight

    DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
    DateTimeFormatter localFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss");

    Logger logger = Logger.getLogger(QueryDecomposition.class.getName());

    public Map<String, Map<String, String>> getL21Mapping(Model model) {
        Map<String, String> range = new HashMap<>();
        range.put("level1", "add_range");
        range.put("level2", "add_range");

        Map<String, String> filter = new HashMap<>();
        filter.put("level1", "none");
        filter.put("level2", "add_filter");

        Map<String, String> keep = new HashMap<>();
        keep.put("level1", "none");
        keep.put("level2", "add_keep");

        Map<String, String> mean = new HashMap<>();
        mean.put("level1", "add_mean");
        mean.put("level2", "add_mean");

        Map<String, String> sum = new HashMap<>();
        sum.put("level1", "add_sum_sumCol");
        sum.put("level2", "add_sumCol");

        Map<String, String> count = new HashMap<>();
        count.put("level1", "add_sumCol");
        count.put("level2", "add_CountCol");

        Map<String, String> min = new HashMap<>();
        min.put("level1", "add_min_minCol");
        min.put("level2", "add_minCol");

        Map<String, String> max = new HashMap<>();
        max.put("level1", "add_max_maxCol");
        max.put("level2", "add_maxCol");

//        Map<String, String> window = new HashMap<>();
//        window.put("level1", "add_window");
//        window.put("level2", "none");

        Map<String, String> aggregateWindow = new HashMap<>();
        aggregateWindow.put("level1", "optional_add_aggregate");
        aggregateWindow.put("level2", "optional_add_aggregate");

        Map<String, String> join = new HashMap<>();
        if ((model == null) || model.equals(Model.FOG)) {
            join.put("level1", "add_join");
            join.put("level2", "add_join");
        } else if (model.equals(Model.EDGE)) {
            join.put("level1", "add_inMemJoin");
            join.put("level2", "add_inMemJoin");
        }

        Map<String, Map<String, String>> mapping = new HashMap<>();
        mapping.put("range", range);
        mapping.put("filter", filter);
        mapping.put("keep", keep);
        mapping.put("mean", mean);
        mapping.put("sum", sum);
        mapping.put("count", count);
        mapping.put("min", min);
        mapping.put("max", max);
        // mapping.put("window", window);
        mapping.put("aggregate_window", aggregateWindow);
        mapping.put("join", join);
        return mapping;
    }

    public CostModelOutput l21decompose(Map<UUID, TSDBQuery> perFogQuery, List<ExecPlan> plan) {

        Set<UUID> devices = perFogQuery.keySet();

        Model model = perFogQuery.get(devices.iterator().next()).getModel();

        Map<String, Map<String, String>> mapping = getL21Mapping(model);
        Map<UUID, String> perFogL2Query;
        Map<UUID, String> perFogL1Query;
        String n = "2";
        perFogL2Query = getLNQuery(mapping, perFogQuery, n, plan);
        n = "1";
        perFogL1Query = getLNQuery(mapping, perFogQuery, n, plan);

        CostModelOutput costModelResult = new CostModelOutput();
        costModelResult.perFogLevel2Query = perFogL2Query;
//        for (String i : perFogL2Query.keySet()) {
//            logger.info(i + " => " + perFogL2Query.get(i));
//        }

        for (UUID i : perFogL1Query.keySet()) {
            logger.info(i + " => " + perFogL1Query.get(i));
            costModelResult.Level1Query = perFogL1Query.get(i);

            // Todo: l1 fog can be changed here. In edge implementation, needs to be hardcoded or get some logic.
//            costModelResult.Level1Fog = coordinatorFog;
            break;
        }
        return costModelResult;
    }

    public Map<UUID, String> getLNQuery(Map<String, Map<String, String>> mapping,
                                          Map<UUID, TSDBQuery> perFogQuery, String leveln, List<ExecPlan> plan) {

        Map<UUID, String> perFogL2Query = new HashMap<>();

        for (UUID i : perFogQuery.keySet()) {
            StringBuilder query = new StringBuilder();
            TSDBQuery TempQu = perFogQuery.get(i);

            if (leveln.equals("2")) {
                query.append("from(bucket:\"").append(TempQu.getBucket()).append("\")");
            } else if (leveln.equals("1")) {
                query.append("from(bucket:\"").append("Level1").append("\")");
            }

            int aggFlag = -1;
            for (String j : TempQu.getOperations().keySet()) {
                if (j.equals("transform")) {
                    j = TempQu.getOperations().get("transform").keySet().toArray()[0].toString();
                }
                switch (mapping.get(j).get("level" + leveln)) {

                    case "add_range":
                        for (String k : TempQu.getOperations().keySet()) {
                            if (k.equals("transform")) {
                                k = TempQu.getOperations().get("transform").keySet().toArray()[0].toString();
                            }
                            if (mapping.get(k).get("level" + leveln).equals("optional_add_aggregate")) {
                                aggFlag = 1;
                            }
                        }

                        LocalDateTime start = LocalDateTime.parse(
                                TempQu.getOperations().get("range").get("start"),
                                DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN));
                        LocalDateTime stop = LocalDateTime.parse(
                                TempQu.getOperations().get("range").get("stop"),
                                DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN));

                        if (leveln.equals("2")) {
                            LocalDateTime temp_date = stop.minusSeconds(1);

                            query.append("|> range(start:").append(start.format(formatter)).append("Z,stop:")
                                    .append(temp_date.format(formatter)).append("Z)");
                        } else if (leveln.equals("1") && aggFlag == 1) {
                            query.append("|> range(start:").append(start.format(formatter)).append("Z,stop:")
                                    .append(stop.format(formatter)).append("Z)");
                        } else if (leveln.equals("1")) {
                            query.append("|>range(start:0)");
                        }
                        break;
                    case "add_filter":
                        query.append("|>filter(fn: (r)=>");
                        List<UUID> temp_mbids = new ArrayList<>();
                        temp_mbids.add(i);
//                        for (ExecPlan e : plan) {
//                            if (e.query.getMechanism() == Model.FOG) {
//                                if (i.equals(e.fog.NodeIP)) {
//                                    temp_mbids.add(e.mbid);
//                                }
//                            } else if (e.query.getMechanism() == Model.EDGE) {
//                                if (i.equals(e.edge.nodeIp)) {
//                                    temp_mbids.add(e.mbid);
//                                }
//                            }
//                        }
                        logger.info("Second Filter mbids: " + temp_mbids);
                        int index = 0;
                        for (UUID id : temp_mbids) {
                            if (index < temp_mbids.size() - 1) {
                                query.append(" r.mbId==\"" + id + "\" or");
                                index++;
                            } else {
                                query.append(" r.mbId==\"" + id + "\"");
                            }
                        }
                        query.append(")");

                        query.append("|> filter( fn: (r) => ");
                        for (Condition k : TempQu.getFilterConditions()) {
                            if (k.getClass().getName().equals((new TagCondition()).getClass().getName())) {
                                TagCondition tag = (TagCondition) k;
                                String operation = "";
                                switch (tag.operation) {
                                    case EQUAL_TO_EQUAL_TO:
                                        operation = "==";
                                        break;
                                    case NOT_EQUAL_TO:
                                        operation = "!=";
                                        break;
                                }
                                if (!tag.operand1.equals("_time")) {
                                    query.append("and r.").append(tag.operand1).append(operation).append("\"")
                                            .append(tag.operand2).append("\" ");
                                } else {
                                    query.append("and r.").append(tag.operand1).append(operation)
                                            .append(tag.operand2);
                                }
                            } else {
                                FieldCondition field = (FieldCondition) k;
                                String operation = "";
                                switch (field.operation) {
                                    case EQUAL_TO_EQUAL_TO:
                                        operation = "==";
                                        break;
                                    case NOT_EQUAL_TO:
                                        operation = "!=";
                                        break;
                                    case GREATER_THAN:
                                        operation = ">";
                                        break;
                                    case LESSER_THAN:
                                        operation = "<";
                                        break;
                                    case LESSER_THAN_EQUAL_TO:
                                        operation = "<=";
                                        break;
                                    case GREATER_THAN_RQUAL_TO:
                                        operation = ">=";
                                        break;
                                }
                                String temp = "";
                                if (Utils.isNumeric(field.operand2_str) == true) {
                                    temp = field.operand2_str;
                                } else {
                                    StringBuilder temp1 = new StringBuilder("\"");
                                    temp1.append(field.operand2_str).append("\"");
                                }
                                query.append("r._field==\"").append(field.operand1).append("\" and r._value")
                                        .append(operation).append(temp);
                            }
                        }
                        query.append(")");
                        break;
                    case "add_keep":
                        query.append("|>keep(columns:[");
                        Map keepMap = TempQu.getOperations().get("keep");
                        for (Object k : keepMap.keySet()) {
                            query.append("\"").append(keepMap.get(k)).append("\"").append(",");
                        }
                        query = new StringBuilder(query.substring(0, query.length() - 1));
                        query.append("])");
                        break;
                    case "add_sumCol":
                    case "add_sum_sumCol":
                        query.append("|>sum()");
                        break;
                    case "add_CountCol":
                        query.append("|>count()");
                        break;
                    case "add_minCol":
                    case "add_min_minCol":
                        query.append("|>min()");
                        break;
                    case "add_maxCol":
                    case "add_max_maxCol":
                        query.append("|>max()");
                        break;
                    case "optional_add_aggregate":
                        String aggregate = TempQu.getOperations().get("transform").get("aggregate_window");
                        String every = StringUtils.substringBefore(aggregate, ",");
                        String fn = StringUtils.substringAfter(aggregate, ",");
                        String intValue = every.replaceAll("[^0-9]", "");
                        if (DELTA % Integer.parseInt(intValue) == 0) {
                            if (leveln.equals("1")) {
                            } else {
                                query.append("|>aggregateWindow(every:").append(every).append(",fn:").append(fn)
                                        .append(")");
                            }
                        } else {
                            aggwin_shortCircuit = 1;
                            query.append("|>aggregateWindow(every:").append(every).append(",fn:").append(fn)
                                    .append(
                                            ")");
                        }
                        break;
                    case "add_mean":
                        shortCirtuit = 1;
                        query.append("|>mean()");
                        break;
                    case "add_inMemJoin":
                        query.append("|>inMemJoin()");
                        break;
                }
            }
            String q = query.toString();
            perFogL2Query.put(i, q);
        }
        return perFogL2Query;
    }
}
