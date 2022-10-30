package com.dreamlab.query;

import com.dreamlab.api.TSDBQuery;
import com.dreamlab.constants.JoinType;

import java.io.Serializable;
import java.util.List;

public class Join implements Serializable {

    public TSDBQuery table1;
    public TSDBQuery table2;
    public List<String> on;
    public JoinType method;

    public Join(List<String> cols, JoinType type) {
        on = cols;
        method = type;
    }

    public Join(TSDBQuery q1, TSDBQuery q2, List<String> cols, JoinType type) {
        table1 = q1;
        table2 = q2;
        on = cols;
        method = type;
    }
}
