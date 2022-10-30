package com.dreamlab.query;

import java.io.Serializable;
import java.util.logging.Logger;

import com.dreamlab.api.Condition;
import com.dreamlab.constants.Operation;

public class TagCondition implements Condition, Cloneable, Serializable {

    private static Logger LOGGER;
    public String operand1 = "nan";
    public Operation operation;
    public String operand2;

    public TagCondition() {
        LOGGER = Logger.getLogger(TagCondition.class.getName());
    }

    public TagCondition(String opr1, Operation op, String opr2) {
        this();
        this.operand1 = opr1;
        this.operation = op;
        this.operand2 = opr2;
    }

    public TagCondition(String opr1, String op, String opr2) {
        this();
        operand1 = opr1;
        operand2 = opr2;
        switch (op) {
            case "==":
                operation = Operation.EQUAL_TO_EQUAL_TO;
                break;
            case "!=":
                operation = Operation.NOT_EQUAL_TO;
                break;
            default:
                LOGGER.info("Operator given, not allowed(tagCondition)");
        }
    }

    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            return new TagCondition(this.operand1, this.operation, this.operand2);
        }
    }
}
