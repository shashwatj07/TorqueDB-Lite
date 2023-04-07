package com.dreamlab.query;

import com.dreamlab.api.Condition;
import com.dreamlab.api.TSDBQuery;
import com.dreamlab.constants.Cache;
import com.dreamlab.constants.JoinType;
import com.dreamlab.constants.Model;
import com.dreamlab.constants.Operation;
import com.dreamlab.constants.QueryPolicy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InfluxDBQuery implements TSDBQuery, Serializable {

    private static final long serialVersionUID = 1462683465307549481L;
    private static Logger LOGGER = Logger.getLogger(InfluxDBQuery.class.getName());
    public String experiment = "default";
    public Model model = null;
    public QueryPolicy queryPolicy = null;
    public Cache cache = null;
    private UUID queryId;
    private String bucket;
    private LinkedHashMap<String, HashMap<String, String>> operations = new LinkedHashMap<>();
    private Join join = null;
    private List<Condition> conditions = new ArrayList<Condition>();

    @Override
    public TSDBQuery clone() {
        InfluxDBQuery temp = new InfluxDBQuery();
        temp.queryId = queryId;
        temp.bucket = bucket;
        temp.operations = operations;
        temp.join = join;
        temp.conditions = conditions;
        temp.experiment = experiment;
        temp.model = model;
        temp.queryPolicy = queryPolicy;
        temp.cache = cache;

        return temp;
    }

    public void addBucketName(String name) {
        this.bucket = name;
    }

    public void addOptionalParameters(Model model, Cache cache,
                                      QueryPolicy queryPolicy) {
        this.model = model;
        this.cache = cache;
        this.queryPolicy = queryPolicy;
    }

    public void addQueryId() {
        this.queryId = UUID.randomUUID();
    }

    public void addQueryId(String queryId) {
        this.queryId = UUID.fromString(queryId);
    }

    public void addRange(String start, String stop) {
        HashMap<String, String> rangeMap = new HashMap<>();
        rangeMap.put("start", start);
        rangeMap.put("stop", stop);
        operations.put("range", rangeMap);
    }

    public void addRegion(String minLat, String maxLat, String minLon, String maxLon) {
        HashMap<String, String> regionMap = new HashMap<>();
        regionMap.put("minLat", minLat);
        regionMap.put("maxLat", maxLat);
        regionMap.put("minLon", minLon);
        regionMap.put("maxLon", maxLon);
        operations.put("region", regionMap);
    }

    public void addFilter(String m_name, List<String> tag_list, List<String> field_list) {
        HashMap<String, String> filterMap = new HashMap<>();

        filterMap.put("measurement", m_name);
        // Edge-JOIN modification
//        filterMap.put("measurement_join", "");

        for (int i = 0; i < tag_list.size(); i++) {
            String tagKey = "tag" + i;
            String tagValue = tag_list.get(i);
            filterMap.put(tagKey, tagValue);
        }

        for (int i = 0; i < field_list.size(); i++) {
            String fieldKey = "field" + i;
            String fieldValue = field_list.get(i);
            filterMap.put(fieldKey, fieldValue);
        }
        operations.put("filter", filterMap);

        Pattern pattern = Pattern.compile("[(==)(!=)]");
        for (String i : tag_list) {
            Matcher matcher = pattern.matcher(i);
            matcher.find();
            int ind1 = Integer.valueOf(matcher.start());
            int ind2 = Integer.valueOf(matcher.end());
            TagCondition tc = new TagCondition(i.substring(0, ind1), i.substring(ind1, ind2 + 1),
                    i.substring(ind2 + 1));
            conditions.add(tc);
        }
        pattern = Pattern.compile("(==|!=|>=|<=|>|<)");
        for (String i : field_list) {
            Matcher matcher = pattern.matcher(i);
            matcher.find();
            int ind1 = Integer.valueOf(matcher.start());
            int ind2 = Integer.valueOf(matcher.end());
            FieldCondition fc;
            fc = new FieldCondition(i.substring(0, ind1), i.substring(ind1, ind2),
                    i.substring(ind2));
            conditions.add(fc);
        }
    }

    public void addKeep(List<String> cols) {
        HashMap<String, String> keepMap = new HashMap<>();
        for (int i = 0; i < cols.size(); i++) {
            String keepKey = new StringBuilder("column").append(i).toString();
            String keepValue = cols.get(i);
            keepMap.put(keepKey, keepValue);
        }
        operations.put("keep", keepMap);
    }

    public void addSum() {
        HashMap<String, String> transformMap = new HashMap<>();
        transformMap.put("sum", null);
        operations.put("transform", transformMap);
    }

    public void addCount() {
        HashMap<String, String> transformMap = new HashMap<>();
        transformMap.put("count", null);
        operations.put("transform", transformMap);
    }

    public void addMin() {
        HashMap<String, String> transformMap = new HashMap<>();
        transformMap.put("min", null);
        operations.put("transform", transformMap);
    }

    public void addMax() {
        HashMap<String, String> transformMap = new HashMap<>();
        transformMap.put("max", null);
        operations.put("transform", transformMap);
    }

    public void addMean() {
        HashMap<String, String> transformMap = new HashMap<>();
        transformMap.put("mean", null);
        operations.put("transform", transformMap);
    }

    public void addWindow(String every, String fn) {
        HashMap<String, String> transformMap = new HashMap<>();
        String value = new StringBuilder(every).append(",").append(fn).toString();
        transformMap.put("aggregate_window", value);
        operations.put("transform", transformMap);
    }

    public void addJoin(InfluxDBQuery q1, InfluxDBQuery q2, List<String> cols,
                        JoinType type) {
        HashMap<String, String> transformMap = new HashMap<>();
        String value = new StringBuilder(String.valueOf(type)).append(",").append(cols).toString();
        join = new Join(q1, q2, cols, type);
        transformMap.put("join", value);
        operations.put("transform", transformMap);
    }

    public void addJoin(List<String> cols, JoinType method) {
        HashMap<String, String> transformMap = new HashMap<>();
        String value = new StringBuilder().append(cols).toString();
        join = new Join(cols, method);
        transformMap.put("join", value);
        operations.put("transform", transformMap);
    }

    public String getBucket() {
        return this.bucket;
    }

    public Join getJoin() {
        return this.join;
    }

    public LinkedHashMap<String, HashMap<String, String>> getOperations() {
        return this.operations;
    }

    public List<Condition> getFilterConditions() {
        return this.conditions;
    }

    public String getExperiment() {
        return this.experiment;
    }

    @Override
    public Model getModel() {
        return this.model;
    }

    @Override
    public QueryPolicy getQueryPolicy() {
        return this.queryPolicy;
    }

//    public Mechanism getMechanism() {
//        return this.mechanism;
//    }
//
//    public Deployment getDeployment() {
//        return this.deployment;
//    }
//
//    public QueryPolicy getQueryPolicy() {
//        return this.queryPolicy;
//    }
//
//    public Caching getCaching() {
//        return caching;
//    }

    public UUID getQueryId() {
        return this.queryId;
    }

    public Set<Condition> getConditions() {
        Set<Condition> s = new HashSet<Condition>();
        for (Condition j : conditions) {
            if (j instanceof TagCondition) {
                TagCondition tc = (TagCondition) j;
                if (tc.operation == Operation.EQUAL_TO_EQUAL_TO) {
                    s.add(tc);
                }
            } else if (j instanceof FieldCondition) {
                FieldCondition fc = (FieldCondition) j;
                if (fc.operation == Operation.EQUAL_TO_EQUAL_TO) {
                    s.add(fc);
                }
            } else {
                LOGGER.info("Throw Exception");
            }
        }
        return (s);
    }

    public List<String> getKeys() {
        List<String> ret = new ArrayList<>();

        for (String key : operations.keySet()) {
            if (key.equals("filter")) {
                for (Condition j : conditions) {
                    if (j instanceof TagCondition) {
                        TagCondition tc = (TagCondition) j;
                        if (tc.operation == Operation.EQUAL_TO_EQUAL_TO) {
                            ret.add(tc.operand1);
                        }
                    } else if (j instanceof FieldCondition) {
                        FieldCondition fc = (FieldCondition) j;
                        if (fc.operation == Operation.EQUAL_TO_EQUAL_TO) {
                            ret.add(fc.operand1);
                        }
                    } else {
                        LOGGER.info("Throw Exception");
                    }
                }
            } else if (key.equals("range")) {
                ret.add("startTS");
                ret.add("endTS");
            }
            else if (key.equals("region")) {
                ret.add("minLat");
                ret.add("maxLat");
                ret.add("minLon");
                ret.add("maxLon");
            }
        }
        return ret;
    }

    public Set<String> getFieldList() {
        for (String key : operations.keySet()) {
            if (key.equals("filter")) {
                Set<String> s = new HashSet<String>();
                s.add("endTS");
                s.add("startTS");
                for (Condition j : conditions) {
                    if (j instanceof FieldCondition) {
                        FieldCondition fc = (FieldCondition) j;
                        s.add(fc.operand1);
                    }
                }
                return (s);
            }
        }
        return new HashSet<String>();
    }

    public String getValueFor() {
        String s = "";
        for (String key : operations.keySet()) {
            if (key.equals("filter")) {
                for (Condition j : conditions) {
                    if (j instanceof FieldCondition) {
                        FieldCondition fc = (FieldCondition) j;
                        s = fc.operand2_str;
                    }
                }
            }
        }
        return s;
    }

    public Operation getOperationForField() {
        Operation s = null;
        for (String key : operations.keySet()) {
            if (key.equals("filter")) {
                for (Condition j : conditions) {
                    if (j instanceof FieldCondition) {
                        FieldCondition fc = (FieldCondition) j;
                        s = fc.operation;
                    }
                }
            }
        }
        return s;
    }

    public String getValueFor(String col) {
        for (String key : operations.keySet()) {
            if (key.equals("filter")) {
                String s = "";
                for (Condition j : conditions) {
                    if (j instanceof TagCondition) {
                        TagCondition tc = (TagCondition) j;
                        if (tc.operation == Operation.EQUAL_TO_EQUAL_TO) {
                            s = tc.operand2;
                        }
                    } else if (j instanceof FieldCondition) {
                        FieldCondition fc = (FieldCondition) j;
                        if (fc.operation == Operation.EQUAL_TO_EQUAL_TO) {
                            s = fc.operand2_str;
                        }
                    } else {
                        LOGGER.info("Throw Exception");
                    }
                }
                return (s);
            }
        }
        return "temp";
    }

    public void removeRange() {
        operations.remove("range");
    }

    public void removeRegion() {
        operations.remove("region");
    }

    public void removeCondition(String col, String value) {
        for (String key : operations.keySet()) {
            if (key.equals("filter")) {
                int index = -1;
                for (Condition j : conditions) {
                    index++;
                    if (j.getClass().equals((new TagCondition()).getClass())) {
                        TagCondition tag = (TagCondition) j;
                        if (tag.operand1.equals(col)) {
                            break;
                        }
                    } else {
                        FieldCondition field = (FieldCondition) j;
                        if (field.operand1.equals(col)) {
                            break;
                        }
                    }
                }
                conditions.remove(index);
            }
        }
    }

    @Override
    public String toString() {
        return "InfluxDBQuery{" +
                "experiment='" + experiment + '\'' +
                ", model=" + model +
                ", queryPolicy=" + queryPolicy +
                ", cache=" + cache +
                ", queryId='" + queryId + '\'' +
                ", bucket='" + bucket + '\'' +
                ", operations=" + operations +
                ", join=" + join +
                ", conditions=" + conditions +
                '}';
    }
}
