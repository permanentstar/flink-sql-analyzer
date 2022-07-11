package com.daasyyds.flink.sql.analyzer.rule;

import com.daasyyds.flink.sql.analyzer.common.Promise;

import java.util.Objects;

public interface Rule<T, R> {
    default String name() {
        return getClass().getSimpleName();
    }
    R apply(T input, RuleContext context);

    /**
    * Rule chain not allow replacing context, if needed, a {@link Promise} should be used.
    * */
    default <S> Rule<T, S> next(Rule<? super R, ? extends S> rule) {
        Objects.requireNonNull(rule);
        return (T t, RuleContext context) -> rule.apply(apply(t, context), context);
    }

    static <T> Rule<T, T> identity() {
        return (T t, RuleContext context) -> t;
    }

}
