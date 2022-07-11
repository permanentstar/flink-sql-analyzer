package com.daasyyds.flink.sql.analyzer.visitor.rel;

import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import com.daasyyds.flink.sql.analyzer.common.AnalyzerException;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;

import java.util.ArrayList;
import java.util.Collection;

public class TableSourceRelationVisitor extends RelVisitor {
    private Collection<TableSourceTable> sources = new ArrayList<>();

    private TableSourceRelationVisitor() {
    }

    public static TableSourceRelationVisitor of() {
        return new TableSourceRelationVisitor();
    }

    public Collection<TableSourceTable> visit(RelNode root) {
        this.go(root);
        return this.sources;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
            RelOptTable table = node.getTable();
            if (!(table instanceof TableSourceTable)) {
                throw new AnalyzerException(String.format("expecting tableScan as TableSourceTable, but receive %s", table.getClass().getSimpleName()));
            }
            this.sources.add((TableSourceTable) table);
        } else {
            node.childrenAccept(this);
        }
    }
}
