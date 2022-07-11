package com.daasyyds.flink.sql.analyzer.rule;

interface String2StringRule extends Rule<String, String> {
    static RuleChain<String> chain(String2StringRule... rules) {
        return RuleChain.of(rules);
    }
}