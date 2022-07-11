package com.daasyyds.flink.sql.analyzer.ability.result;

public class TranslatedStatement extends SingleStatement {
    private String newSql;
    private boolean translated;

    public TranslatedStatement() {
    }

    public TranslatedStatement(String sqlType, String sql, int index, String newSql, boolean translated) {
        super(sqlType, sql, index);
        this.newSql = newSql;
        this.translated = translated;
    }

    public String getNewSql() {
        return newSql;
    }

    public boolean isTranslated() {
        return translated;
    }
}
