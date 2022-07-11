package com.daasyyds.flink.sql.analyzer.semantic;

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;

public class SourceOrSinkObject {
    private IdentifierLike identifier;
    private boolean isTemporary;
    private boolean isView;
    private boolean isSink;

    private SourceOrSinkObject() {
    }

    private SourceOrSinkObject(IdentifierLike identifier, boolean isTemporary, boolean isView, boolean isSink) {
        this.identifier = identifier;
        this.isTemporary = isTemporary;
        this.isView = isView;
        this.isSink = isSink;
    }

    public static SourceOrSinkObject of(IdentifierLike identifier, boolean isTemporary, boolean isView, boolean isSink) {
        return new SourceOrSinkObject(identifier, isTemporary, isView, isSink);
    }

    public IdentifierLike getIdentifier() {
        return identifier;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public boolean isView() {
        return isView;
    }

    public boolean isSink() {
        return isSink;
    }
}
