package com.daasyyds.flink.sql.analyzer.visitor.parse;

import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;

import com.daasyyds.flink.sql.analyzer.common.AnalyzerException;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorTable;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
* result include table & view
* */
public class CatalogSourceVisitor extends SqlBasicVisitor<Collection<CatalogSchemaTable>> {
    private FlinkCalciteSqlValidator validator;
    private static final Field tableInNamespace;
    private static final String tableNamespaceClz = "org.apache.calcite.sql.validate.TableNamespace";

    static {
        try {
            Class<?> clz = SqlValidatorNamespace.class.getClassLoader().loadClass(tableNamespaceClz);
            tableInNamespace = clz.getDeclaredField("table");
            tableInNamespace.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private CatalogSourceVisitor() {
    }

    private CatalogSourceVisitor(FlinkCalciteSqlValidator validator) {
        this.validator = validator;
    }

    public static CatalogSourceVisitor of(FlinkCalciteSqlValidator validator) {
        return new CatalogSourceVisitor(validator);
    }

    @Override
    public Collection<CatalogSchemaTable> visit(SqlCall call) {
        return combinePreparingTables(call.getOperandList());
    }

    @Override
    public Collection<CatalogSchemaTable> visit(SqlNodeList nodeList) {
        return combinePreparingTables(nodeList.getList());
    }

    private Collection<CatalogSchemaTable> combinePreparingTables(List<SqlNode> nodes) {
        return nodes.stream().filter(Objects::nonNull).flatMap(o -> {
            Collection<CatalogSchemaTable> fptbs = o.accept(CatalogSourceVisitor.this);
            return fptbs == null ? Stream.empty() : fptbs.stream();
        }).collect(Collectors.toList());
    }

    @Override
    public Collection<CatalogSchemaTable> visit(SqlIdentifier id) {
        try {
            SqlValidatorNamespace namespace = validator.getNamespace(id);
            if (namespace != null) {
                namespace = namespace.resolve();
                if (namespace.getClass().getName().equals(tableNamespaceClz)) {
                    SqlValidatorTable validatorTable = (SqlValidatorTable)tableInNamespace.get(namespace);
                    if (!(validatorTable instanceof RelOptTableImpl)) {
                        throw new AnalyzerException(String.format("expect RelOptTableImpl for SqlValidatorTable but receive %s", validatorTable.getClass().getName()));
                    }
                    CatalogSchemaTable table = validatorTable.unwrap(CatalogSchemaTable.class);
                    if (table == null) {
                        throw new AnalyzerException(String.format("table field of RelOptTableImpl should be CatalogSchemaTable, while current RelOptTableImpl is %s", validatorTable));
                    }
                    return Collections.singletonList(table);
                }
            }
            return null;
        } catch (IllegalAccessException e) {
            throw new AnalyzerException(e);
        }
    }

}
