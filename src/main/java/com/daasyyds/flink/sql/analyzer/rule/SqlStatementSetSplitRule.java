package com.daasyyds.flink.sql.analyzer.rule;

import com.daasyyds.flink.sql.analyzer.calcite.DialectParser;
import com.daasyyds.flink.sql.analyzer.common.AnalyzerException;
import com.daasyyds.flink.sql.analyzer.common.StringUtil;
import com.daasyyds.flink.sql.analyzer.common.Tuple2;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class SqlStatementSetSplitRule implements FlatMapRule<String, Tuple2<SqlCall, String>> {
    private static final Pattern statementSetPattern = Pattern.compile("(COMPILE\\s+AND\\s+EXECUTE\\s+PLAN\\s+'[^']*?'\\s+FOR|EXECUTE|COMPILE\\s+PLAN\\s+'[^']*?'\\s+FOR)\\s+STATEMENT\\s+SET\\s+BEGIN\\s+((?:INSERT.*?;)+)\\s*END", CASE_INSENSITIVE | Pattern.DOTALL);
    private DialectParser dialectParser;

    private SqlStatementSetSplitRule(DialectParser dialectParser) {
        this.dialectParser = dialectParser;
    }

    public static SqlStatementSetSplitRule of(DialectParser dialectParser) {
        return new SqlStatementSetSplitRule(dialectParser);
    }

    @Override
    public Collection<Tuple2<SqlCall, String>> apply(String sql, RuleContext context) {
        Collection<Tuple2<SqlCall, String>> flat = new ArrayList<>();
        Matcher matcher = statementSetPattern.matcher(sql);
        int startPos = 0, endPos = 0;
        try {
            while (matcher.find()) {
                startPos = matcher.start();
                String before = StringUtil.trim(sql.substring(endPos, startPos).trim(), ";");
                if (StringUtil.notEmpty(before)) {
                    flat.addAll(dialectParser.splitSqlByPos(before));
                }
                String ss = matcher.group();
                SqlCall[] calls = dialectParser.parseSqlNode(ss);
                assert calls.length == 1;
                flat.add(Tuple2.of(calls[0], ss));
                endPos = matcher.end();
            }
            if (endPos < sql.length()) {
                String tail = StringUtil.trim(sql.substring(endPos).trim(), ";");
                if (StringUtil.notEmpty(tail)) {
                    flat.addAll(dialectParser.splitSqlByPos(tail));
                }
            }
        } catch (SqlParseException e) {
            throw new AnalyzerException(e.getCause());
        }
        return flat;
    }
}
