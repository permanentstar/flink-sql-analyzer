package com.daasyyds.flink.sql.analyzer.ability.setting.hint;

import java.util.Collection;
import java.util.stream.Collectors;

public abstract class HintOperator<T extends HintOperator.HintOption> {
    private String name;
    private Collection<T> options;
    private OperatorType operatorType;

    public HintOperator(String name, Collection<T> options, OperatorType operatorType) {
        this.name = name;
        this.options = options;
        this.operatorType = operatorType;
    }

    public final String expression() {
        if (options == null || options.isEmpty()) {
            return name;
        }
        String optionStr = options.stream().map(HintOption::toLiteral).collect(Collectors.joining(", "));
        return String.format("%s(%s)", name, optionStr);
    }

    @Override
    public String toString() {
        return String.format("/*+ %s */", expression());
    }


    public interface HintOption {
        String toLiteral();
    }

    public String getName() {
        return name;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public Collection<T> getOptions() {
        return options;
    }

    public enum OperatorType {
        TABLE_HINT,
        QUERY_HINT
    }
}
