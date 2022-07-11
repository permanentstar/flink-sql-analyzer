package com.daasyyds.flink.sql.analyzer.ability.setting.hint;

public class KeyValueOption implements HintOperator.HintOption {
    private String key;
    private String value;

    public KeyValueOption(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toLiteral() {
        return String.format("'%s'='%s'", key, value);
    }

    @Override
    public String toString() {
        return "KeyValueOption{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
