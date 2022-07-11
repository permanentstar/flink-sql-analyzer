package com.daasyyds.flink.sql.analyzer;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import com.daasyyds.flink.sql.analyzer.ability.Parse;
import com.daasyyds.flink.sql.analyzer.ability.result.ExplainedStatement;
import com.daasyyds.flink.sql.analyzer.ability.result.SingleStatement;
import com.daasyyds.flink.sql.analyzer.ability.result.SourceSinks;
import com.daasyyds.flink.sql.analyzer.ability.setting.ExplainSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.ParseSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.SourceSinkSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.ValidateSetting;
import com.daasyyds.flink.sql.analyzer.calcite.ParsedStatement;
import com.daasyyds.flink.sql.analyzer.common.AnalyzerException;
import com.daasyyds.flink.sql.analyzer.common.Promise;
import com.daasyyds.flink.sql.analyzer.common.StopWatch;
import com.daasyyds.flink.sql.analyzer.common.Tuple2;
import com.daasyyds.flink.sql.analyzer.config.CatalogConf;
import com.daasyyds.flink.sql.analyzer.planner.PlannerPool;
import com.daasyyds.flink.sql.analyzer.planner.SemanticRuleContext;
import com.daasyyds.flink.sql.analyzer.rule.CollectRule;
import com.daasyyds.flink.sql.analyzer.rule.CommentRemoveRule;
import com.daasyyds.flink.sql.analyzer.rule.CreateDropResolveRule;
import com.daasyyds.flink.sql.analyzer.rule.OperationResolveRule;
import com.daasyyds.flink.sql.analyzer.rule.Rule;
import com.daasyyds.flink.sql.analyzer.rule.RuleChain;
import com.daasyyds.flink.sql.analyzer.rule.RuleContext;
import com.daasyyds.flink.sql.analyzer.rule.SemanticDatasources;
import com.daasyyds.flink.sql.analyzer.rule.SourceSinkResolveRule;
import com.daasyyds.flink.sql.analyzer.rule.SplitMergeRule;
import com.daasyyds.flink.sql.analyzer.rule.SqlStatementSetSplitRule;
import com.daasyyds.flink.sql.analyzer.semantic.ResolvedSemantic;
import org.apache.calcite.sql.SqlCall;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FlinkAnalyzer implements Parse {
    private PlannerPool pool;

    private FlinkAnalyzer() {}

    private FlinkAnalyzer(CatalogConf conf) {
        pool = new PlannerPool(conf);
    }

    public static FlinkAnalyzer newInstance(CatalogConf conf) {
        return new FlinkAnalyzer(conf);
    }

    @Override
    public boolean validate(String sqls, ValidateSetting setting) {
        try {
            resolveSemantic(sqls, setting, RuleChain.identity());
        } catch (Exception e) {
            if (setting.isThrowIfInvalid()) {
                throw new AnalyzerException(e);
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public Collection<SingleStatement> splitStatements(String sqls, ParseSetting setting) {
        List<Tuple2<SqlCall, String>> res = new ArrayList<>(CommentRemoveRule.INSTANCE
                .next(SqlStatementSetSplitRule.of(pool.getDialectParser()))
                .apply(sqls, RuleContext.emptyContext()));
        return IntStream.range(0, res.size())
                .mapToObj(index -> new ParsedStatement(res.get(index).f0.getOperator().getName(), res.get(index).f1, index, res.get(index).f0))
                .collect(Collectors.toList());
    }

    @Override
    public Collection<SourceSinks> sourceSinks(String sqls, SourceSinkSetting setting) {
        Promise<Collection<ParsedStatement>, Collection<ResolvedSemantic>> semanticPromise = semanticProc(sqls, setting, SourceSinkResolveRule.of(setting.isRecurseViewToTableSources()));
        Promise<Collection<ResolvedSemantic>, Collection<SourceSinks>> collectPromise = semanticPromise.split((rss, context) -> rss,
                () -> SemanticDatasources.of(new ResolvedSemantic.ResolveOverall(semanticPromise.getResult()).reduceCreateDropObjects(),setting));
        Stream<SourceSinks> datasources = collectPromise.realizeAndResolve().stream();
        if (setting.isIgnoreNoDatasourceStatement()) {
            datasources = datasources.filter(ds -> !ds.getDatasources().isEmpty());
        }
        return datasources.collect(Collectors.toList());
    }

    @Override
    public Collection<ExplainedStatement> explain(String sqls, ExplainSetting setting) {
        Promise<Collection<ParsedStatement>, Collection<ExplainedStatement>> promise = semanticProc(sqls, setting, (rs, context) -> {
            SemanticRuleContext src = (SemanticRuleContext) context;
            Operation op = rs.getOperation();
            String explainStr = null;
            if (op instanceof PlannerQueryOperation || op instanceof SinkModifyOperation || op instanceof StatementSetOperation) {
                explainStr = src.getPlanner().explain(Collections.singletonList(op), ExplainDetail.CHANGELOG_MODE, ExplainDetail.JSON_EXECUTION_PLAN);
            }
            return new ExplainedStatement(rs.getSqlType(), rs.getSql(), rs.getSqlIndex(), explainStr, explainStr != null);
        });
        Collection<ExplainedStatement> explained = promise.realizeAndResolve();
        if (setting.isIgnoreNoExplainable()) {
            explained = explained.stream().filter(ExplainedStatement::isExplained).collect(Collectors.toList());
        }
        return explained;
    }

    public ResolvedSemantic.ResolveOverall resolveSemantic(String sqls, ParseSetting setting, RuleChain<ResolvedSemantic> proc) {
        StopWatch stopWatch = StopWatch.start();
        Promise<Collection<ParsedStatement>, Collection<ResolvedSemantic>> promise = semanticProc(sqls, setting, proc);
        Collection<ResolvedSemantic> rss = promise.realizeAndResolve();
        return new ResolvedSemantic.ResolveOverall(rss, new ResolvedSemantic.ResolveStatistics(stopWatch.stop()));
    }

    private <T> Promise<Collection<ParsedStatement>, Collection<T>> semanticProc(String sqls, ParseSetting setting, Rule<ResolvedSemantic, T> proc) {
        Collection<SingleStatement> statements = splitStatements(sqls, setting);
        return semanticProc(statements.stream().map(ParsedStatement.class::cast).collect(Collectors.toList()), setting, proc);
    }

    private <T> Promise<Collection<ParsedStatement>, Collection<T>> semanticProc(Collection<ParsedStatement> ss, ParseSetting setting, Rule<ResolvedSemantic, T> proc) {
        assert proc != null;
        Rule<ResolvedSemantic, T> semanticProcRules = RuleChain.of(
                OperationResolveRule.INSTANCE,
                CreateDropResolveRule.INSTANCE
        ).next(proc);
        Rule<Collection<ParsedStatement>, Collection<T>> plannerProcRules = SplitMergeRule.of(
                (sts, context) -> sts.stream().map(ResolvedSemantic::init).collect(Collectors.toList()),
                semanticProcRules,
                CollectRule.identity()
        );
        return pool.inContext(ss, plannerProcRules, setting);
    }

}
