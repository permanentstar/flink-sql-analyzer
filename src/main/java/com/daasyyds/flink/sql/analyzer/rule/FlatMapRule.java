package com.daasyyds.flink.sql.analyzer.rule;

import java.util.Collection;
import java.util.Collections;

public interface FlatMapRule<T, R> extends Rule<T, Collection<R>> {

    static <T> FlatMapRule<T, T> identity() {
        return (T t, RuleContext context) -> Collections.singletonList(t);
    }
}
