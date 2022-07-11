package com.daasyyds.flink.sql.analyzer.parse;

import com.daasyyds.flink.sql.analyzer.TestUtil;
import com.daasyyds.flink.sql.analyzer.calcite.DialectParser;
import com.daasyyds.flink.sql.analyzer.calcite.FlinkDialectParser;
import com.daasyyds.flink.sql.analyzer.common.Tuple2;
import com.daasyyds.flink.sql.analyzer.rule.CommentRemoveRule;
import com.daasyyds.flink.sql.analyzer.rule.RuleContext;
import com.daasyyds.flink.sql.analyzer.rule.SqlStatementSetSplitRule;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SqlSplitTest {

    private final DialectParser parser = new FlinkDialectParser();

    @Test
    public void testMultiSelect() {
        assertSplitEqual(Arrays.asList(
                "/sql/parse/flink/dql/sql1.sql",
                "/sql/parse/flink/dql/sql2.sql",
                "/sql/parse/flink/dql/sql3.sql",
                "/sql/parse/flink/dql/sql4.sql",
                "/sql/parse/flink/dql/sql5.sql"
        ));
    }

    @Test
    public void testMultiInsert() {
        assertSplitEqual(Arrays.asList(
                "/sql/parse/flink/dml/sql1.sql",
                "/sql/parse/flink/dml/sql2.sql",
                "/sql/parse/flink/dml/sql3.sql",
                "/sql/parse/flink/dml/sql4.sql"
        ));
    }

    @Test
    public void testMultiTableCommand() {
        assertSplitEqual(Arrays.asList(
                "/sql/parse/flink/cmd/table/sql1.sql",
                "/sql/parse/flink/cmd/table/sql2.sql",
                "/sql/parse/flink/cmd/table/sql3.sql",
                "/sql/parse/flink/cmd/table/sql4.sql"
        ));
    }

    @Test
    public void testMultiCatalogAndDatabaseCommand() {
        assertSplitEqual(Arrays.asList(
                "/sql/parse/flink/cmd/catalog/sql1.sql",
                "/sql/parse/flink/cmd/catalog/sql2.sql",
                "/sql/parse/flink/cmd/catalog/sql3.sql",
                "/sql/parse/flink/cmd/database/sql1.sql",
                "/sql/parse/flink/cmd/database/sql2.sql",
                "/sql/parse/flink/cmd/database/sql3.sql"
        ));
    }

    @Test
    public void testMultiFunctionCommand() {
        assertSplitEqual(Arrays.asList(
                "/sql/parse/flink/cmd/function/sql1.sql",
                "/sql/parse/flink/cmd/function/sql2.sql",
                "/sql/parse/flink/cmd/function/sql3.sql"
        ));
    }

    @Test
    public void testOtherCmd() {
        assertSplitEqual(Arrays.asList(
                "/sql/parse/flink/cmd/other/sql1.sql",
                "/sql/parse/flink/cmd/other/sql2.sql",
                "/sql/parse/flink/cmd/other/sql3.sql",
                "/sql/parse/flink/cmd/other/sql4.sql",
                "/sql/parse/flink/cmd/other/sql5.sql"
        ));
    }

    @Test
    public void testComplexProcedure() {
        assertSplitEqual(Arrays.asList(
                "/sql/parse/flink/cmd/database/sql2.sql",
                "/sql/parse/flink/cmd/table/sql1.sql",
                "/sql/parse/flink/dml/sql4.sql",
                "/sql/parse/flink/cmd/table/sql4.sql"
        ));
    }

    private void assertSplitEqual(List<String> paths) {
        Tuple2<String, List<String>> sqlsToTest = TestUtil.sqlToTest(paths);
        assertSplitEqual(sqlsToTest.f0, sqlsToTest.f1.toArray(new String[0]));
    }

    private void assertSplitEqual(String sqls, String... expects) {
        String[] splits = CommentRemoveRule.INSTANCE
                .next(SqlStatementSetSplitRule.of(parser))
                .apply(sqls, RuleContext.EMPTY_CONTEXT)
                .stream()
                .map(Tuple2::getF1).toArray(String[]::new);
        Assert.assertArrayEquals(expects, splits);
    }


}
