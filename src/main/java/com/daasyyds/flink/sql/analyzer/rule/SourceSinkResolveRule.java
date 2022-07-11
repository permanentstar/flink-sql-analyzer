package com.daasyyds.flink.sql.analyzer.rule;

import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlStatementSet;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.operations.CompileAndExecutePlanOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;
import com.daasyyds.flink.sql.analyzer.common.AnalyzerException;
import com.daasyyds.flink.sql.analyzer.planner.PlannerUtil;
import com.daasyyds.flink.sql.analyzer.planner.SemanticRuleContext;
import com.daasyyds.flink.sql.analyzer.semantic.ResolvedSemantic;
import com.daasyyds.flink.sql.analyzer.semantic.SourceOrSinkObject;
import com.daasyyds.flink.sql.analyzer.visitor.parse.CatalogSourceVisitor;
import com.daasyyds.flink.sql.analyzer.visitor.parse.NearestNodeVisitor;
import com.daasyyds.flink.sql.analyzer.visitor.rel.TableSourceRelationVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.daasyyds.flink.sql.analyzer.planner.PlannerUtil.catalogObject2Identifier;

public class SourceSinkResolveRule implements Rule<ResolvedSemantic, ResolvedSemantic> {
    private boolean recurseViewToTableSources;

    private SourceSinkResolveRule() {
    }

    private SourceSinkResolveRule(boolean recurseViewToTableSources) {
        this.recurseViewToTableSources = recurseViewToTableSources;
    }

    public static SourceSinkResolveRule of(boolean recurseViewToTableSources) {
        return new SourceSinkResolveRule(recurseViewToTableSources);
    }

    @Override
    public ResolvedSemantic apply(ResolvedSemantic rs, RuleContext context) {
        List<SourceOrSinkObject> sso = collectSourceSink(rs.getOperation(), rs.getNode(), context).stream().distinct().collect(Collectors.toList());
        return rs.resolveSourceOrSinkObjects(sso);
    }

    private List<SourceOrSinkObject> collectSourceSink(Operation op, SqlNode node, RuleContext context) {
        List<SourceOrSinkObject> ssos = new ArrayList<>();
        if (op instanceof SinkModifyOperation) {
            SinkModifyOperation smo = (SinkModifyOperation) op;
            ContextResolvedTable crt = smo.getContextResolvedTable();
            IdentifierLike il = catalogObject2Identifier(crt.getIdentifier());
            ssos.add(SourceOrSinkObject.of(il, crt.isTemporary(), crt.getResolvedTable() instanceof ResolvedCatalogView, true));
            if (smo.getChild() instanceof PlannerQueryOperation) {
                ssos.addAll(collectSourceSink(smo.getChild(), ((SqlInsert) node).getSource(), context));
            }
        } else if (op instanceof PlannerQueryOperation) {
            if (recurseViewToTableSources) { // no views will be found
                RelNode rn = ((PlannerQueryOperation) op).getCalciteTree();
                Collection<TableSourceTable> sources = TableSourceRelationVisitor.of().visit(rn);
                sources.forEach(t -> {
                    IdentifierLike source = IdentifierLike.qualified(t.contextResolvedTable().getIdentifier().asSummaryString());
                    ssos.add(SourceOrSinkObject.of(source, t.contextResolvedTable().isTemporary(), t.contextResolvedTable().getResolvedTable() instanceof ResolvedCatalogView, false));
                });
            } else {
                SemanticRuleContext src = (SemanticRuleContext) context;
                ParserImpl parser = (ParserImpl)src.getPlanner().getParser();
                FlinkPlannerImpl flinkPlanner = PlannerUtil.getFlinkPlanner(parser);
                CatalogSourceVisitor csv = CatalogSourceVisitor.of(flinkPlanner.getOrCreateSqlValidator());
                flinkPlanner.validate(node).accept(csv).forEach(t -> {
                    IdentifierLike source = IdentifierLike.qualified(t.getContextResolvedTable().getIdentifier().asSummaryString());
                    ssos.add(SourceOrSinkObject.of(source, t.getContextResolvedTable().isTemporary(), t.getContextResolvedTable().getResolvedTable() instanceof ResolvedCatalogView, false));
                });
            }
        } else if (op instanceof CreateViewOperation) {
            CreateViewOperation operation = (CreateViewOperation) op;
            CatalogView cv = operation.getCatalogView();
            SqlNode recursiveNode = ((SqlCreateView) node).getQuery();
            if (cv instanceof QueryOperationCatalogView) {
                ssos.addAll(collectSourceSink(((QueryOperationCatalogView) cv).getQueryOperation(), recursiveNode, context));
            } else {
                String subQuery = operation.getCatalogView().getExpandedQuery();
                Operation subOperation = ((SemanticRuleContext) context).getPlanner().getParser().parse(subQuery).get(0);
                ssos.addAll(collectSourceSink(subOperation, recursiveNode, context));
            }
        } else if (op instanceof StatementSetOperation) {
            StatementSetOperation ss = (StatementSetOperation) op;
            nearest(node, SqlStatementSet.class).ifPresent(sqlStatementSet -> {
                List<RichSqlInsert> inserts = sqlStatementSet.getInserts();
                IntStream.range(0, inserts.size()).forEach(i -> {
                    ModifyOperation mo = ss.getOperations().get(i);
                    List<SourceOrSinkObject> objects = collectSourceSink(mo, inserts.get(i), context);
                    ssos.addAll(objects);
                });
            });

        } else if (op instanceof CompileAndExecutePlanOperation) {
            CompileAndExecutePlanOperation cepo = (CompileAndExecutePlanOperation) op;
            SqlNode recursiveNode = null;
            Optional<SqlStatementSet> sss = nearest(node, SqlStatementSet.class);
            if (sss.isPresent()) {
                recursiveNode = sss.get();
            } else {
                Optional<SqlInsert> si = nearest(node, SqlInsert.class);
                if (!si.isPresent()) {
                    throw new AnalyzerException("neither SqlStatementSet nor SqlInsert find in SqlCompileAndExecutePlan");
                }
                recursiveNode = si.get();
            }
            ssos.addAll(collectSourceSink(cepo.getOperation(), recursiveNode, context));
        }
        return ssos;
    }

    private <T extends SqlNode> Optional<T> nearest(SqlNode parent, Class<T> clz) {
        return Optional.ofNullable(parent.accept(NearestNodeVisitor.of(clz)));
    }
}
