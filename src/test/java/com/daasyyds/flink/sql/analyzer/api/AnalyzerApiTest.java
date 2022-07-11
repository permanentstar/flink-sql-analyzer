package com.daasyyds.flink.sql.analyzer.api;

import org.apache.flink.table.catalog.exceptions.CatalogException;

import com.daasyyds.flink.sql.analyzer.FlinkAnalyzer;
import com.daasyyds.flink.sql.analyzer.TestUtil;
import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;
import com.daasyyds.flink.sql.analyzer.ability.result.SourceSinks;
import com.daasyyds.flink.sql.analyzer.ability.setting.ExplainSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.ParseSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.SourceSinkSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.ValidateSetting;
import com.daasyyds.flink.sql.analyzer.common.AnalyzerException;
import com.daasyyds.flink.sql.analyzer.common.Tuple2;
import com.daasyyds.flink.sql.analyzer.config.CatalogConf;
import com.daasyyds.flink.sql.analyzer.rule.RuleChain;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class AnalyzerApiTest {
    private FlinkAnalyzer fa = FlinkAnalyzer.newInstance(getConf());

    private CatalogConf getConf() {
        CatalogConf.Catalog catalog = new CatalogConf.Catalog("test_catalog", new HashMap<String, String>() {{
            put("default-database", "default_database");
            put("type", "generic_in_memory");
        }});
        CatalogConf.Module module = new CatalogConf.Module("hive");
        return new CatalogConf(Collections.singletonList(catalog), Collections.singletonList(module));
    }

    @Test
    public void testValidation() {
        ValidateSetting vs = new ValidateSetting(Collections.emptyList(), "default_catalog", "default_database", true);
        Assert.assertTrue(fa.validate(singleFileSqls("/sql/api/flink/sql1.sql"), vs));
        Assert.assertTrue(fa.validate(singleFileSqls("/sql/api/flink/sql2.sql"), vs));
        Assert.assertTrue(fa.validate(singleFileSqls("/sql/api/flink/sql3.sql"), vs));
        Assert.assertTrue(fa.validate(singleFileSqls("/sql/api/flink/sql4.sql"), vs));
    }

    @Test
    public void testUnsupportResolveOperation() {
        ParseSetting ps = new ParseSetting();
        String sql = singleFileSqls("/sql/api/flink/sql4.sql");
        AnalyzerException ae = Assert.assertThrows(AnalyzerException.class, () -> fa.resolveSemantic(sql + ";\nUSE CATALOG should_not_success", ps, RuleChain.identity()));
        Assert.assertTrue((ae.getCause() instanceof CatalogException) && ae.getCause().getMessage().contains("[should_not_success] does not exist"));
    }

    @Test
    public void testSourceSink() {
        checkDatasource1();
        checkDatasource2();
        checkDatasource3();
    }

    @Test
    public void testExplain() {
        ExplainSetting es = new ExplainSetting();
        fa.explain(singleFileSqls("/sql/api/flink/sql3.sql"), es);
    }

    @Test
    public void testWithClause() {
        SourceSinkSetting sss = new SourceSinkSetting(Collections.emptyList(), "default_catalog", "default_database",
                true, true, true, true, false, true);
        String sql = singleFileSqls("/sql/api/flink/sql5.sql");
        fa.sourceSinks(sql, sss);
    }

    private void checkDatasource1() {
        SourceSinkSetting sss = new SourceSinkSetting(Collections.emptyList(), "default_catalog", "default_database",
                true, true, true, true, false, false);
        Tuple2<String, List<String>> sqlParts = singleFileSqlParts("/sql/api/flink/sql1.sql");
        List<SourceSinks> sourceSinks = new ArrayList<>(fa.sourceSinks(sqlParts.f0, sss));
        Assert.assertEquals(sourceSinks.size(), sqlParts.f1.size());
        for(int i=0;i<sourceSinks.size();++i) {
            if (i <=3) {
                Assert.assertEquals(0, sourceSinks.get(i).getDatasources().size());
            } else {
                List<SourceSinks.Datasource> objects = new ArrayList<>(sourceSinks.get(4).getDatasources());
                Assert.assertEquals(2, objects.size());
                SourceSinks.Datasource sink = objects.get(0);
                Assert.assertEquals("test_catalog.default_database.blackhole_table", IdentifierLike.fullQualifier(sink));
                Assert.assertTrue(sink.isCreated() && !sink.isDropped() && sink.isSink() && sink.isTemporary() && !sink.isView());
                SourceSinks.Datasource source = objects.get(1);
                Assert.assertEquals("test_catalog.default_database.datagen_table", IdentifierLike.fullQualifier(source));
                Assert.assertTrue(source.isCreated() && !source.isDropped() && !source.isSink() && source.isTemporary() && !source.isView());
            }
        }
    }

    private void checkDatasource2() {
        SourceSinkSetting sss = new SourceSinkSetting(Collections.emptyList(), "default_catalog", "default_database",
                true, true, false, true, false, true);
        String sql = singleFileSqls("/sql/api/flink/sql2.sql");
        List<SourceSinks> sourceSinks = new ArrayList<>(fa.sourceSinks(sql, sss));
        Assert.assertEquals(1, sourceSinks.size());
        SourceSinks ss = sourceSinks.get(0);
        List<SourceSinks.Datasource> objects = new ArrayList<>(ss.getDatasources());
        Assert.assertEquals(2, objects.size());
        SourceSinks.Datasource sink = objects.get(0);
        Assert.assertEquals("default_catalog.default_database.dwa_behave_agg", IdentifierLike.fullQualifier(sink));
        Assert.assertTrue(sink.isCreated() && !sink.isDropped() && sink.isSink() && !sink.isTemporary() && !sink.isView());
        SourceSinks.Datasource source = objects.get(1);
        Assert.assertEquals("default_catalog.default_database.dwd_user_behave", IdentifierLike.fullQualifier(source));
        Assert.assertTrue(source.isCreated() && !source.isDropped() && !source.isSink() && !source.isTemporary() && !source.isView());
    }

    private void checkDatasource3() {
        SourceSinkSetting sss = new SourceSinkSetting(Collections.emptyList(), "default_catalog", "default_database",
                true, false, true, true, false, true);
        String sql = singleFileSqls("/sql/api/flink/sql3.sql");
        List<SourceSinks> sourceSinks = new ArrayList<>(fa.sourceSinks(sql, sss));
        Assert.assertEquals(4, sourceSinks.size());

        // datagen_table_1 created and dropped in sequence, so regard as transient
        List<SourceSinks.Datasource> object1 = new ArrayList<>(sourceSinks.get(0).getDatasources());
        Assert.assertEquals(1, object1.size());
        SourceSinks.Datasource source1 = object1.get(0);
        Assert.assertEquals("test_catalog.default_database.datagen_table_1", IdentifierLike.fullQualifier(source1));
        Assert.assertTrue(!source1.isCreated() && !source1.isDropped() && !source1.isSink() && source1.isTemporary() && !source1.isView());

        // SELECT addNum(f_sequence, f_random) FROM datagen_table_2
        List<SourceSinks.Datasource> object2 = new ArrayList<>(sourceSinks.get(1).getDatasources());
        Assert.assertEquals(1, object2.size());
        SourceSinks.Datasource source2 = object2.get(0);
        Assert.assertEquals("default_catalog.default_database.datagen_table_2", IdentifierLike.fullQualifier(source2));
        Assert.assertTrue(source2.isCreated() && !source2.isDropped() && !source2.isSink() && source2.isTemporary() && !source2.isView());

        // CREATE VIEW datagen_view AS SELECT * FROM datagen_table_2
        List<SourceSinks.Datasource> object3 = new ArrayList<>(sourceSinks.get(2).getDatasources());
        Assert.assertEquals(1, object3.size());
        SourceSinks.Datasource source3 = object3.get(0);
        Assert.assertEquals("default_catalog.default_database.datagen_table_2", IdentifierLike.fullQualifier(source3));
        Assert.assertTrue(source3.isCreated() && !source3.isDropped() && !source3.isSink() && source3.isTemporary() && !source3.isView());

        // SELECT * FROM datagen_view
        List<SourceSinks.Datasource> object4 = new ArrayList<>(sourceSinks.get(3).getDatasources());
        Assert.assertEquals(1, object4.size());
        SourceSinks.Datasource source4 = object4.get(0);
        Assert.assertEquals("default_catalog.default_database.datagen_view", IdentifierLike.fullQualifier(source4));
        Assert.assertTrue(source4.isCreated() && !source4.isDropped() && !source4.isSink() && !source4.isTemporary() && source4.isView());
    }

    private String singleFileSqls(String path) {
        return TestUtil.combineSqls(Collections.singletonList(path));
    }

    private Tuple2<String, List<String>> singleFileSqlParts(String path) {
        return TestUtil.sqlToTest(Collections.singletonList(path));
    }
}
