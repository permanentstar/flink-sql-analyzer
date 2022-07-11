package com.daasyyds.flink.sql.analyzer.ability.result;

public class SingleStatement {
    private String sqlType;
    private String sql;
    private int sqlIndex;

    public SingleStatement() {
    }

    public SingleStatement(String sqlType, String sql, int sqlIndex) {
        this.sqlType = sqlType;
        this.sql = sql;
        this.sqlIndex = sqlIndex;
    }

    public String getSqlType() {
        return sqlType;
    }

    public String getSql() {
        return sql;
    }

    public int getSqlIndex() {
        return sqlIndex;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setSqlIndex(int sqlIndex) {
        this.sqlIndex = sqlIndex;
    }
}
