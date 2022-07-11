package com.daasyyds.flink.sql.analyzer.rule;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RuleChain<T> implements Rule<T, T> {
    private List<Rule<T, T>> rules;

    private RuleChain() {}

    private RuleChain(List<Rule<T, T>> rules) {
        this.rules = rules;
    }

    public static <T> RuleChain<T> identity() {
        return RuleChain.of(Rule.identity());
    }

    @SafeVarargs
    public static <T> RuleChain<T> of(Rule<T, T>... rules) {
        assert rules != null && rules.length > 0;
        return new RuleChain<>(Stream.of(rules).collect(Collectors.toList()));
    }

    public RuleChain<T> addRule(Rule<T, T> rule) {
        this.rules.add(rule);
        return this;
    }

    @Override
    public T apply(T input, RuleContext context) {
        Iterator<Rule<T, T>> iterator = rules.iterator();
        Rule<T, T> rule = iterator.next();
        while (iterator.hasNext()) {
            rule = rule.next(iterator.next());
        }
        return rule.apply(input, context);
    }
}
