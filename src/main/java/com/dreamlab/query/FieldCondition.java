package com.dreamlab.query;

import java.io.Serializable;
import java.util.logging.Logger;

import com.dreamlab.api.Condition;
import com.dreamlab.constants.Operation;

public class FieldCondition implements Condition, Cloneable, Serializable {

    private static Logger LOGGER;
    public String operand1 = "nan";
    public Operation operation;
    public String operand2_str;

    public FieldCondition() {
        LOGGER = Logger.getLogger(FieldCondition.class.getName());
    }

    public FieldCondition(String opr1, Operation op, String opr2) {
        this();
        this.operand1 = opr1;
        this.operation = op;
        this.operand2_str = opr2;
    }

    public FieldCondition(String opr1, String op, String opr2) {
        this();
        operand1 = opr1;
        operand2_str = opr2;
        //logger.info("from FIeldCondition Class op ="+op+op.length());
        switch (op) {
            case "==":
                operation = Operation.EQUAL_TO_EQUAL_TO;
                break;
            case "!=":
                operation = Operation.NOT_EQUAL_TO;
                break;
            case ">":
                operation = Operation.GREATER_THAN;
                break;
            case "<":
                operation = Operation.LESSER_THAN;
                break;
            case "<=":
                operation = Operation.LESSER_THAN_EQUAL_TO;
                break;
            case ">=":
                operation = Operation.GREATER_THAN_RQUAL_TO;
                break;
            default:
                LOGGER.info("Operator given, not allowed(field condition)");
        }
    }

    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            return new FieldCondition(this.operand1, this.operation, this.operand2_str);
        }
    }
}
