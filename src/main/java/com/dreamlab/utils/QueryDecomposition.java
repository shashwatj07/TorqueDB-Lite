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

    Logger LOGGER;

    public QueryDecomposition(Logger logger) {
        LOGGER = logger;
    }

    public Map<String, Map<String, String>> getL21Mapping(Model model) {
        Map<String, String> range = new HashMap<>();
        range.put("level2", "add_range");

        Map<String, String> region = new HashMap<>();
        region.put("level2", "add_region");

        Map<String, String> filter = new HashMap<>();
        filter.put("level2", "add_filter");

        Map<String, String> keep = new HashMap<>();
        keep.put("level2", "add_keep");

        Map<String, String> mean = new HashMap<>();
        mean.put("level2", "add_mean");

        Map<String, String> sum = new HashMap<>();
        sum.put("level2", "add_sumCol");

        Map<String, String> count = new HashMap<>();
        count.put("level2", "add_CountCol");

        Map<String, String> min = new HashMap<>();
        min.put("level2", "add_minCol");

        Map<String, String> max = new HashMap<>();
        max.put("level2", "add_maxCol");

//        Map<String, String> window = new HashMap<>();
//        window.put("level2", "none");

        Map<String, String> aggregateWindow = new HashMap<>();
        aggregateWindow.put("level2", "optional_add_aggregate");

        Map<String, String> join = new HashMap<>();
        if ((model == null) || model.equals(Model.FOG)) {
            join.put("level2", "add_join");
        } else if (model.equals(Model.EDGE)) {
            join.put("level2", "add_inMemJoin");
        }

        Map<String, Map<String, String>> mapping = new HashMap<>();
        mapping.put("range", range);
        mapping.put("region", region);
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

//        Model model = perFogQuery.get(devices.iterator().next()).getModel();
        Model model = null;

        Map<String, Map<String, String>> mapping = getL21Mapping(model);
        Map<UUID, List<String>> perFogL2Query = getLNQuery(mapping, perFogQuery, plan);

        CostModelOutput costModelResult = new CostModelOutput();
        costModelResult.perFogLevel2Query = perFogL2Query;

        return costModelResult;
    }

    public Map<UUID, List<String>> getLNQuery(Map<String, Map<String, String>> mapping,
                                          Map<UUID, TSDBQuery> perFogQuery, List<ExecPlan> plan) {

        Map<UUID, List<String>> perFogL2Query = new HashMap<>();

        for (UUID i : perFogQuery.keySet()) {
            List<UUID> temp_mbids = new ArrayList<>();
            for (ExecPlan execPlan : plan) {
                if (execPlan.getFogId().equals(i)) {
                    temp_mbids.add(execPlan.getBlockId());
                }
            }
            LOGGER.info(String.format("%s[Count %s] CoordinatorServer.blocksPerFog: %d", LOGGER.getName(), perFogQuery.get(i).getQueryId(), temp_mbids.size()));
            System.out.println("# Blocks Per Query: " + temp_mbids.size());
            final int num_blocks_per_query = 150;
            for (int n = 0; n < temp_mbids.size(); n += num_blocks_per_query) {
                StringBuilder query = new StringBuilder();
                TSDBQuery TempQu = perFogQuery.get(i);
                query.append("import \"experimental/geo\"\n");
                query.append("from(bucket:\"").append(TempQu.getBucket()).append("\")");

                for (String j : TempQu.getOperations().keySet()) {
                    if (j.equals("transform")) {
                        j = TempQu.getOperations().get("transform").keySet().toArray()[0].toString();
                    }
                    switch (mapping.get(j).get("level2")) {

                        case "add_range":

                            LocalDateTime start = LocalDateTime.parse(
                                    TempQu.getOperations().get("range").get("start"),
                                    DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN)).minusMinutes(330);
                            LocalDateTime stop = LocalDateTime.parse(
                                    TempQu.getOperations().get("range").get("stop"),
                                    DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN)).minusMinutes(330);

                            LocalDateTime temp_date = stop.minusSeconds(1);

                            query.append("|> range(start:").append(start.format(formatter)).append("Z,stop:")
                                    .append(temp_date.format(formatter)).append("Z)");
                            break;
                        case "add_region":
                            HashMap<String, String> region = TempQu.getOperations().get("region");
                            query.append("|> geo.filterRows(region: ")
                                    .append("{ minLat: ").append(region.get("minLat"))
                                    .append(", maxLat: ").append(region.get("maxLat"))
                                    .append(", minLon: ").append(region.get("minLon"))
                                    .append(", maxLon: ").append(region.get("maxLon"))
                                    .append(" }, strict: true)");
                            break;
                        case "add_filter":
                            query.append("|> filter(fn: (r)=>");
//                        List<UUID> temp_mbids = new ArrayList<>();
//                        for (ExecPlan execPlan : plan) {
//                            if (execPlan.getFogId().equals(i)) {
//                                temp_mbids.add(execPlan.getBlockId());
//                            }
//                        }
//                        System.out.println("# Blocks Per Query: " + temp_mbids.size());
                            int index = 0;
                            for (int k = n; k < Math.min(n + num_blocks_per_query, temp_mbids.size()); k++) {
                                UUID id = temp_mbids.get(k);
                                if (index < Math.min(n + num_blocks_per_query, temp_mbids.size()) - 1) {
                                    query.append(" r.blockid==\"").append(id).append("\" or");
                                    index++;
                                } else {
                                    query.append(" r.blockid==\"").append(id).append("\"");
                                }
                            }
                            query.append(")");

                            for (Condition k : TempQu.getFilterConditions()) {
                                query.append("|> filter( fn: (r) => ");
                                if (k.getClass().getName().equals(TagCondition.class.getName())) {
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
                                        query.append("r.").append(tag.operand1).append(operation).append("\"")
                                                .append(tag.operand2).append("\" ");
                                    } else {
                                        query.append("r.").append(tag.operand1).append(operation)
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
                                    if (Utils.isNumeric(field.operand2_str)) {
                                        temp = field.operand2_str;
                                    } else {
                                        StringBuilder temp1 = new StringBuilder("\"");
                                        temp1.append(field.operand2_str).append("\"");
                                    }
                                    query.append("r._field==\"").append(field.operand1).append("\" and r._value")
                                            .append(operation).append(temp);
                                }
                                query.append(")");
                            }
                            break;
                        case "add_keep":
                            query.append("|>keep(columns:[");
                            Map<String, String> keepMap = TempQu.getOperations().get("keep");
                            for (String k : keepMap.keySet()) {
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
                                query.append("|>aggregateWindow(every:").append(every).append(",fn:").append(fn)
                                        .append(")");
                            } else {
                                aggwin_shortCircuit = 1;
                                query.append("|>aggregateWindow(every:")
                                        .append(every).append(",fn:").append(fn).append(")");
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
                if (!perFogL2Query.containsKey(i)) {
                    perFogL2Query.put(i, new ArrayList<>());
                }
                perFogL2Query.get(i).add(q);
            }
        }
        return perFogL2Query;
    }
}
