package com.daasyyds.flink.sql.analyzer.rule;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public class SplitMergeRule<T, R, N, S> implements Rule<T, S> {
    private FlatMapRule<T, R> flatMapRule;
    private Rule<R, N> transformRule;
    private CollectRule<N, S> collectRule;

    private SplitMergeRule() {
    }

    private SplitMergeRule(FlatMapRule<T, R> flatMapRule, Rule<R, N> transformRule, CollectRule<N, S> collectRule) {
        this.flatMapRule = flatMapRule;
        this.transformRule = transformRule;
        this.collectRule = collectRule;
    }

    public static <T, R, N, S> SplitMergeRule<T, R, N, S> of(FlatMapRule<T, R> flatMapRule, Rule<R, N> mapChain, CollectRule<N, S> collectRule) {
        return new SplitMergeRule<>(flatMapRule, mapChain, collectRule);
    }

    @Override
    public S apply(T input, RuleContext context) {
        Collection<N> collectionR = flatMapRule
                .apply(input, context)
                .stream()
                .map(r -> transformRule.apply(r, context))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return collectRule.apply(collectionR, context);
    }
}
