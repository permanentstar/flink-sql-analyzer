package com.daasyyds.flink.sql.analyzer.ability.setting.hint;

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;

import java.util.Collection;

public class OptionsHint extends HintOperator<KeyValueOption> implements TableAware {
    private static final String NAME = "OPTIONS";

    private IdentifierLike hintOn;

    public OptionsHint(IdentifierLike hintOn, Collection<KeyValueOption> options) {
        super(NAME, options, OperatorType.TABLE_HINT);
        this.hintOn = hintOn;
    }

    @Override
    public IdentifierLike aware() {
        return getHintOn();
    }

    public IdentifierLike getHintOn() {
        return hintOn;
    }
}
