package com.daasyyds.flink.sql.analyzer.calcite;

import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;

public class HiveDialectParser extends DialectParser {
    @Override
    protected SqlParser.Config parserConfig() {
        SqlConformance conformance = FlinkSqlConformance.HIVE;
        return SqlParser.config()
                .withParserFactory(FlinkSqlParserFactories.create(conformance))
                .withConformance(conformance)
                .withLex(Lex.JAVA)
                .withIdentifierMaxLength(256);
    }

    @Override
    protected SqlDialect dialect() {
        return SqlDialect.HIVE;
    }
}
