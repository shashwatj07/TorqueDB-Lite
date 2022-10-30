package com.dreamlab.api;

//import com.dreamlab.QueryClasses.Caching;
//import com.dreamlab.QueryClasses.Deployment;
import com.dreamlab.constants.Operation;
import com.dreamlab.query.Join;
//import com.dreamlab.QueryClasses.Mechanism;
//import com.dreamlab.QueryClasses.Operation;
//import com.dreamlab.QueryClasses.QueryPolicy;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public interface TSDBQuery extends Serializable {

    String getBucket();

    String getQueryId();

    Join getJoin();

    HashMap<String, HashMap<String, String>> getOperations();

    Set<Condition> getConditions();

    String getExperiment();

//    Mechanism getMechanism();
//
//    Deployment getDeployment();
//
//    QueryPolicy getQueryPolicy();
//
//    Caching getCaching();

    List<Condition> getFilterConditions();

    List<String> getKeys();

    Set<String> getFieldList();

    String getValueFor();

    String getValueFor(String col);

    Operation getOperationForField();

    TSDBQuery clone();

    void removeRange();

    void removeCondition(String col, String s);
}
