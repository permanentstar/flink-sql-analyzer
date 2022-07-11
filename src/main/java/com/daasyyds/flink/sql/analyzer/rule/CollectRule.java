package com.daasyyds.flink.sql.analyzer.rule;

import java.util.Collection;

public interface CollectRule<N, S> extends Rule<Collection<N>, S> {

    static <N> CollectRule<N, Collection<N>> identity() {
        return (c, context) -> c;
    }
}
