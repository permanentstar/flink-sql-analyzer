package com.daasyyds.flink.sql.analyzer.visitor.parse;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.Objects;
import java.util.Optional;

public class NearestNodeVisitor<T extends SqlNode> extends SqlBasicVisitor<T> {
    private Class<T> tClass;

    private NearestNodeVisitor(Class<T> tClass) {
        this.tClass = tClass;
    }

    public static <T extends SqlNode> NearestNodeVisitor<T> of(Class<T> tClass) {
        return new NearestNodeVisitor<>(tClass);
    }

    @Override
    public T visit(SqlLiteral literal) {
        return literal.getClass() == tClass ? tClass.cast(literal) : null;
    }

    @Override
    public T visit(SqlCall call) {
        if (call.getClass() == tClass) {
            return tClass.cast(call);
        }
        Optional<T> opt = call.getOperandList().stream().map(o -> o.accept(this)).filter(Objects::nonNull).findFirst();
        return opt.orElse(null);
    }

    @Override
    public T visit(SqlNodeList nodeList) {
        return nodeList.getList().get(0).accept(this);
    }

    @Override
    public T visit(SqlIdentifier id) {
        return id.getClass() == tClass ? tClass.cast(id) : null;
    }

    @Override
    public T visit(SqlDataTypeSpec type) {
        return type.getClass() == tClass ? tClass.cast(type) : null;
    }

    @Override
    public T visit(SqlDynamicParam param) {
        return param.getClass() == tClass ? tClass.cast(param) : null;
    }

    @Override
    public T visit(SqlIntervalQualifier intervalQualifier) {
        return intervalQualifier.getClass() == tClass ? tClass.cast(intervalQualifier) : null;
    }
}
