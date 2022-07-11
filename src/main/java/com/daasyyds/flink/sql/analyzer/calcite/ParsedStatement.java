package com.daasyyds.flink.sql.analyzer.calcite;

import com.daasyyds.flink.sql.analyzer.ability.result.SingleStatement;
import org.apache.calcite.sql.SqlNode;

public class ParsedStatement extends SingleStatement {
    private transient SqlNode node;

    public ParsedStatement(String sqlType, String sql, int sqlIndex, SqlNode node) {
        super(sqlType, sql, sqlIndex);
        this.node = node;
    }

    public SqlNode getNode() {
        return node;
    }

}
