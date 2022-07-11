package com.daasyyds.flink.sql.analyzer.rule;

public interface RuleContext {
    RuleContext EMPTY_CONTEXT = () -> {};
    RuleContext INHERIT_CONTEXT = () -> {};
    void depose();

    static RuleContext emptyContext() {
        return EMPTY_CONTEXT;
    }

    static RuleContext inheritContext() {
        return INHERIT_CONTEXT;
    }
}
