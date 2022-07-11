package com.daasyyds.flink.sql.analyzer.rule;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CommentRemoveRule implements String2StringRule {
    private static final String COMMENT_PREFIX = "--";
    public static final CommentRemoveRule INSTANCE = new CommentRemoveRule();
    private CommentRemoveRule() {
    }

    @Override
    public String apply(String input, RuleContext context) {
        return Stream.of(input.split("\n", -1)).filter(l -> !l.trim().startsWith(COMMENT_PREFIX)).collect(Collectors.joining("\n"));
    }
}
