package com.daasyyds.flink.sql.analyzer.ability.setting;

import com.daasyyds.flink.sql.analyzer.ability.setting.hint.HintOperator;

public abstract class HintAction {
    private HintOperator<?> operator;

    // todo: for now, only replace type supported
    private HintOverwriteType overwriteType = HintOverwriteType.REPLACE;

    public HintAction(HintOperator<?> operator) {
        this.operator = operator;
    }

    public HintOperator<?> getOperator() {
        return operator;
    }

    public HintOverwriteType getOverwriteType() {
        return overwriteType;
    }

    public enum HintOverwriteType {
        REPLACE,
        MERGE
    }

}
