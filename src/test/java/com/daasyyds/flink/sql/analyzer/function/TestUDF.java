package com.daasyyds.flink.sql.analyzer.function;

import org.apache.flink.table.functions.ScalarFunction;

public class TestUDF extends ScalarFunction {

    public Integer eval(Integer a, Integer b) {
        return a + b;
    }
}
