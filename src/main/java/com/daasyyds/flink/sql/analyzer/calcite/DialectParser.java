package com.daasyyds.flink.sql.analyzer.calcite;

import org.apache.flink.table.api.SqlDialect;

import com.daasyyds.flink.sql.analyzer.common.AnalyzerException;
import com.daasyyds.flink.sql.analyzer.common.Tuple2;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static com.daasyyds.flink.sql.analyzer.common.StringUtil.splitStr;
import static com.daasyyds.flink.sql.analyzer.common.StringUtil.trim;

public abstract class DialectParser {

    protected abstract SqlParser.Config parserConfig();
    protected abstract SqlDialect dialect();

    public final boolean match(String sqlDialect) {
        return dialect().name().toLowerCase().equals(sqlDialect.toLowerCase());
    }

    public static DialectParser discoverParser(ClassLoader classLoader, String dialect) {
        List<DialectParser> dps = new ArrayList<>();
        ServiceLoader.load(DialectParser.class, classLoader).forEach(parser -> {
            if (parser.match(dialect)) {
                dps.add(parser);
            }
        });
        if (dps.isEmpty()) {
            throw new RuntimeException(String.format(
                    "Could not find any dialectParser that implement '%s' by dialect '%s' in the classpath.",
                    DialectParser.class.getName(), dialect));
        } else if (dps.size() > 1) {
            throw new RuntimeException(String.format(
                    "Multiple dialectParser for dialect '%s' that implement '%s' found in the classpath.\n\n"
                            + "Ambiguous classes are:\n\n"
                            + "%s",
                    dialect,
                    DialectParser.class.getName(),
                    dps.stream()
                            .map(f -> f.getClass().getName())
                            .sorted()
                            .collect(Collectors.joining("\n"))));
        }
        return dps.get(0);
    }

    public final SqlCall[] parseSqlNode(String sqls) throws SqlParseException {
        SqlParser sqlParser = SqlParser.create(sqls, parserConfig());
        SqlNodeList sqlNodeList = sqlParser.parseStmtList();
        SqlNode[] nodes = sqlNodeList.getList().toArray(new SqlNode[0]);
        for (SqlNode n : nodes) {
            if (!(n instanceof SqlCall)) {
                throw new UnsupportedOperationException("input sqls should only accept SqlCalls which maybe command/ddl/dql/dml ..");
            }
        }
        return Arrays.stream(nodes).map(SqlCall.class::cast).toArray(SqlCall[]::new);
    }

    public final List<Tuple2<SqlCall, String>> splitSqlByPos(String sqls) throws SqlParseException {
        SqlCall[] nodes = parseSqlNode(sqls);
        int sqlSize = nodes.length;
        int[] endLines = new int[sqlSize];
        int[] endPos = new int[sqlSize];
        SqlCall node;
        SqlParserPos spp;
        for(int i = 0;i < sqlSize;++i) {
            node = nodes[i];
            spp = node.getParserPosition().plusAll(node.getOperandList());
            endLines[i] = spp.getEndLineNum();
            endPos[i] = spp.getEndColumnNum();
        }
        List<String> splits = splitByLinePos(sqls, endLines, endPos);
        if (splits.size() != sqlSize) {
            throw new AnalyzerException(String.format("sql nodes size %d not equal to split size %d", sqlSize, splits.size()));
        }
        List<Tuple2<SqlCall, String>> res = new ArrayList<>(sqlSize);
        for (int i = 0; i < sqlSize; ++i) {
            res.add(new Tuple2<>(nodes[i], splits.get(i).trim()));
        }
        return res;
    }

    private static List<String> splitByLinePos(String sqls, int[] endLine, int[] endPos) {
        assert endLine.length == endPos.length;
        List<String> segments = new ArrayList<>();
        List<String> lines = splitStr(sqls, "\n");
        StringBuilder sb = new StringBuilder();
        int k = 0;
        int posInLine = 0;
        for(int i = 0; i< endLine.length; ++i) {
            for(int j = k; j< lines.size(); ++j, ++k) {
                if (j + 1 < endLine[i]) {
                    sb.append(lines.get(j).substring(posInLine)).append("\n");
                    posInLine = 0;
                } else {
                    sb.append(lines.get(j), posInLine, endPos[i]);
                    segments.add(trim(sb.toString(), ";"));
                    sb = new StringBuilder();
                    if (endPos[i] < lines.get(j).length()) {
                        posInLine = endPos[i];
                    } else {
                        posInLine = 0;
                        ++k;
                    }
                    break;
                }
            }
        }
        return segments;
    }


}
