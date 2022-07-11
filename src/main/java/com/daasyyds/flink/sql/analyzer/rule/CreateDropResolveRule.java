package com.daasyyds.flink.sql.analyzer.rule;

import org.apache.flink.table.operations.Operation;
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

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;
import com.daasyyds.flink.sql.analyzer.semantic.CreateOrDropObject;
import com.daasyyds.flink.sql.analyzer.semantic.ResolvedSemantic;

import java.util.ArrayList;
import java.util.List;

import static com.daasyyds.flink.sql.analyzer.planner.PlannerUtil.catalogObject2Identifier;

public class CreateDropResolveRule implements Rule<ResolvedSemantic, ResolvedSemantic> {
    public static final CreateDropResolveRule INSTANCE = new CreateDropResolveRule();

    private CreateDropResolveRule() {
    }

    @Override
    public ResolvedSemantic apply(ResolvedSemantic rs, RuleContext context) {
        List<CreateOrDropObject> createdOrDropped = new ArrayList<>();
        Operation operation = rs.getOperation();
        if (operation instanceof CreateOperation) {
            if (operation instanceof CreateViewOperation) {
                CreateViewOperation cvo = (CreateViewOperation) operation;
                createdOrDropped.add(CreateOrDropObject.of(catalogObject2Identifier(cvo.getViewIdentifier()), CreateOrDropObject.ObjectType.VIEW, cvo.isTemporary(), false));
            } else if (operation instanceof CreateTableOperation) {
                CreateTableOperation cto = (CreateTableOperation) operation;
                createdOrDropped.add(CreateOrDropObject.of(catalogObject2Identifier(cto.getTableIdentifier()), CreateOrDropObject.ObjectType.TABLE, cto.isTemporary(), false));
            } else if (operation instanceof CreateTableASOperation) {
                CreateTableOperation cto = ((CreateTableASOperation) operation).getCreateTableOperation();
                createdOrDropped.add(CreateOrDropObject.of(catalogObject2Identifier(cto.getTableIdentifier()), CreateOrDropObject.ObjectType.TABLE, cto.isTemporary(), false));
            } else if (operation instanceof CreateTempSystemFunctionOperation) {
                CreateTempSystemFunctionOperation ctsfo = (CreateTempSystemFunctionOperation) operation;
                createdOrDropped.add(CreateOrDropObject.of(IdentifierLike.ofAnonymous(ctsfo.getFunctionName()), CreateOrDropObject.ObjectType.FUNCTION, true, false));
            } else if (operation instanceof CreateCatalogFunctionOperation) {
                CreateCatalogFunctionOperation ccfo = (CreateCatalogFunctionOperation) operation;
                createdOrDropped.add(CreateOrDropObject.of(catalogObject2Identifier(ccfo.getFunctionIdentifier()), CreateOrDropObject.ObjectType.FUNCTION, ccfo.isTemporary(), false));
            }
        } else if (operation instanceof DropOperation) {
            if (operation instanceof DropTableOperation) {
                DropTableOperation dp = (DropTableOperation) operation;
                createdOrDropped.add(CreateOrDropObject.of(catalogObject2Identifier(dp.getTableIdentifier()), CreateOrDropObject.ObjectType.TABLE, dp.isTemporary(), true));
            } else if (operation instanceof DropViewOperation) {
                DropViewOperation dvo = (DropViewOperation) operation;
                createdOrDropped.add(CreateOrDropObject.of(catalogObject2Identifier(dvo.getViewIdentifier()), CreateOrDropObject.ObjectType.VIEW, dvo.isTemporary(), true));
            } else if (operation instanceof DropTempSystemFunctionOperation) {
                DropTempSystemFunctionOperation dtsf = (DropTempSystemFunctionOperation) operation;
                createdOrDropped.add(CreateOrDropObject.of(IdentifierLike.ofAnonymous(dtsf.getFunctionName()), CreateOrDropObject.ObjectType.FUNCTION, true, true));
            } else if (operation instanceof DropCatalogFunctionOperation) {
                DropCatalogFunctionOperation dcf = (DropCatalogFunctionOperation) operation;
                createdOrDropped.add(CreateOrDropObject.of(catalogObject2Identifier(dcf.getFunctionIdentifier()), CreateOrDropObject.ObjectType.FUNCTION, dcf.isTemporary(), true));
            }
        }
        return rs.resolveCreatedOrDroppedObjects(createdOrDropped);
    }
}
