package com.daasyyds.flink.sql.analyzer.ability.setting.hint;

public class LiteralOption implements HintOperator.HintOption {
    private Object literal;

    public LiteralOption(Object literal) {
        this.literal = literal;
    }

    @Override
    public String toLiteral() {
        return literal.toString();
    }

    @Override
    public String toString() {
        return "LiteralOption{" +
                "literal=" + literal +
                '}';
    }
}
