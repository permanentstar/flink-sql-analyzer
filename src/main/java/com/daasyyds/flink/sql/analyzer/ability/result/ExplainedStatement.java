package com.daasyyds.flink.sql.analyzer.ability.result;

public class ExplainedStatement extends SingleStatement {
    private String explanation;
    private boolean explained;

    public ExplainedStatement() {
    }

    public ExplainedStatement(String sqlType, String sql, int sqlIndex, String explanation, boolean explained) {
        super(sqlType, sql, sqlIndex);
        this.explanation = explanation;
        this.explained = explained;
    }

    public String getExplanation() {
        return explanation;
    }

    public void setExplanation(String explanation) {
        this.explanation = explanation;
    }

    public boolean isExplained() {
        return explained;
    }

    public void setExplained(boolean explained) {
        this.explained = explained;
    }
}
