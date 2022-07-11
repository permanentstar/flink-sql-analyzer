package com.daasyyds.flink.sql.analyzer.rule;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateOperation;
import org.apache.flink.table.operations.ddl.CreateTableASOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropViewOperation;
import org.apache.flink.table.planner.delegation.PlannerBase;

import com.daasyyds.flink.sql.analyzer.common.AnalyzerException;
import com.daasyyds.flink.sql.analyzer.config.AnalyzerOptions;
import com.daasyyds.flink.sql.analyzer.planner.SemanticRuleContext;
import com.daasyyds.flink.sql.analyzer.semantic.ResolvedSemantic;

import java.util.Optional;

import static com.daasyyds.flink.sql.analyzer.config.AnalyzerOptions.SEMANTIC_RESOLVE_IGNORE_UNSUPPORTED;

public class OperationResolveRule  implements Rule<ResolvedSemantic, ResolvedSemantic> {
    public static final OperationResolveRule INSTANCE = new OperationResolveRule();

    private OperationResolveRule() {}

    @Override
    public ResolvedSemantic apply(ResolvedSemantic rs, RuleContext context) {
        try {
            SemanticRuleContext src = (SemanticRuleContext) context;
            PlannerBase pb = src.getPlanner();
            CatalogManager cm = pb.catalogManager();
            Operation operation = pb.getParser().parse(rs.getSql()).get(0);
            // simulate catalog context switch
            if (operation instanceof UseCatalogOperation) {
                UseCatalogOperation uco = (UseCatalogOperation) operation;
                cm.setCurrentCatalog(uco.getCatalogName());
            } else if (operation instanceof UseDatabaseOperation) {
                UseDatabaseOperation udo = (UseDatabaseOperation) operation;
                cm.setCurrentCatalog(udo.getCatalogName());
                cm.setCurrentDatabase(udo.getDatabaseName());
            } else if (operation instanceof CreateOperation) {
                if (operation instanceof CreateViewOperation) {
                    simulateCreateView((CreateViewOperation) operation, cm);
                } else if (operation instanceof CreateTableOperation) {
                    simulateCreateTable((CreateTableOperation) operation, cm);
                } else if (operation instanceof CreateTableASOperation) {
                    CreateTableOperation cto = ((CreateTableASOperation) operation).getCreateTableOperation();
                    simulateCreateTable(cto, cm);
                } else if (operation instanceof CreateTempSystemFunctionOperation) {
                    CreateTempSystemFunctionOperation ctsfo = (CreateTempSystemFunctionOperation) operation;
                    pb.functionCatalog().registerTemporarySystemFunction(ctsfo.getFunctionName(), ctsfo.getCatalogFunction(), ctsfo.isIgnoreIfExists());
                } else if (operation instanceof CreateCatalogFunctionOperation) {
                    CreateCatalogFunctionOperation ccfo = (CreateCatalogFunctionOperation) operation;
                    simulateCreateCatalogFunction(ccfo, pb.functionCatalog());
                } else {
                    checkUnsupportedOperation(operation, src.getConfiguration());
                }
            } else if (operation instanceof DropOperation) {
                if (operation instanceof DropTableOperation) {
                    DropTableOperation op = (DropTableOperation) operation;
                    cm.dropTemporaryTable(op.getTableIdentifier(), op.isIfExists());
                } else if (operation instanceof DropViewOperation) {
                    DropViewOperation op = (DropViewOperation) operation;
                    cm.dropTemporaryView(op.getViewIdentifier(), op.isIfExists());
                } else if (operation instanceof DropTempSystemFunctionOperation) {
                    DropTempSystemFunctionOperation op = (DropTempSystemFunctionOperation) operation;
                    pb.functionCatalog().dropTemporarySystemFunction(op.getFunctionName(), op.isIfExists());
                } else if (operation instanceof DropCatalogFunctionOperation) {
                    DropCatalogFunctionOperation op = (DropCatalogFunctionOperation) operation;
                    pb.functionCatalog().dropTempCatalogFunction(op.getFunctionIdentifier(), op.isIfExists());
                } else {
                    checkUnsupportedOperation(operation, src.getConfiguration());
                }
            } else {
                checkUnsupportedOperation(operation, src.getConfiguration());
            }
            return rs.resolveOperation(operation);
        } catch (Exception e) {
            throw new AnalyzerException(String.format("parse failed at sql index %d(index based from 0)", rs.getSqlIndex()), e);
        }
    }

    private void checkUnsupportedOperation(Operation operation, Configuration configuration) {
        if (!configuration.get(SEMANTIC_RESOLVE_IGNORE_UNSUPPORTED)) {
            if (!AnalyzerOptions.isSemanticResolveSupported(operation.getClass(), configuration)) {
                throw new AnalyzerException(String.format("semantic resolve unsupported operation %s", operation.getClass().getName()));
            }
        }
    }

    private void simulateCreateView(CreateViewOperation cvo, CatalogManager cm) throws TableAlreadyExistException {
        ObjectIdentifier oi = cvo.getViewIdentifier();
        if (cvo.isTemporary()) { // maybe a bit tricky here, since we can not find out where the exist one came from, just replace it.
            cm.dropTemporaryView(oi, true);
            cm.createTemporaryTable(cvo.getCatalogView(), oi, cvo.isIgnoreIfExists());
        } else { // in case catalog view, we can't actually perform creation to catalog, just replace temporary table with the identifier.
            Optional<ContextResolvedTable> opt = cm.getTable(oi);
            if (!cvo.isIgnoreIfExists() && opt.isPresent()) {
                throw new TableAlreadyExistException(oi.getCatalogName(), new ObjectPath(oi.getDatabaseName(), oi.getObjectName()));
            } else if (!opt.isPresent()) {
                cm.dropTemporaryView(oi, true);
                cm.createTemporaryTable(cvo.getCatalogView(), oi, cvo.isIgnoreIfExists());
            }
        }
    }

    private void simulateCreateTable(CreateTableOperation cto, CatalogManager cm) throws TableAlreadyExistException {
        ObjectIdentifier oi = cto.getTableIdentifier();
        if (cto.isTemporary()) { // maybe a bit tricky here, since we can not find out where the exist one came from, just replace it.
            cm.dropTemporaryTable(oi, true);
            cm.createTemporaryTable(cto.getCatalogTable(), oi, cto.isIgnoreIfExists());
        } else { // in case catalog table, we can't actually perform creation to catalog, just replace temporary table with the identifier.
            Optional<ContextResolvedTable> opt = cm.getTable(oi);
            if (!cto.isIgnoreIfExists() && opt.isPresent()) {
                throw new TableAlreadyExistException(oi.getCatalogName(), new ObjectPath(oi.getDatabaseName(), oi.getObjectName()));
            } else if (!opt.isPresent()) {
                cm.dropTemporaryView(oi, true);
                cm.createTemporaryTable(cto.getCatalogTable(), oi, cto.isIgnoreIfExists());
            }
        }
    }

    private void simulateCreateCatalogFunction(CreateCatalogFunctionOperation ccfo, FunctionCatalog fc) {
        ObjectIdentifier oi = ccfo.getFunctionIdentifier();
        fc.dropTempCatalogFunction(oi, true);
        fc.registerTemporaryCatalogFunction(UnresolvedIdentifier.of(oi), ccfo.getCatalogFunction(), ccfo.isIgnoreIfExists());
    }
}
