package com.daasyyds.flink.sql.analyzer.semantic;

import org.apache.flink.table.operations.Operation;

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;
import com.daasyyds.flink.sql.analyzer.calcite.ParsedStatement;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ResolvedSemantic extends ParsedStatement {
    private Operation operation;
    private List<CreateOrDropObject> createdOrDropped;
    private List<SourceOrSinkObject> sourceOrSink;

    public ResolvedSemantic(ParsedStatement statement) {
        super(statement.getSqlType(), statement.getSql(), statement.getSqlIndex(), statement.getNode());
    }

    public static ResolvedSemantic init(ParsedStatement statement) {
        return new ResolvedSemantic(statement);
    }

    public ResolvedSemantic resolveOperation(Operation op) {
        this.operation = op;
        return this;
    }

    public Operation getOperation() {
        return operation;
    }

    public List<CreateOrDropObject> getCreatedOrDropped() {
        return createdOrDropped;
    }

    public List<SourceOrSinkObject> getSourceOrSink() {
        return sourceOrSink;
    }

    public ResolvedSemantic resolveCreatedOrDroppedObjects(List<CreateOrDropObject> createdOrDropped) {
        this.createdOrDropped = createdOrDropped;
        return this;
    }

    public ResolvedSemantic resolveSourceOrSinkObjects(List<SourceOrSinkObject> sourceOrSink) {
        this.sourceOrSink = sourceOrSink;
        return this;
    }

    public static class ResolveOverall {
        private Collection<ResolvedSemantic> resolved;
        private ResolveStatistics statistics;

        public ResolveOverall() {
        }

        public ResolveOverall(Collection<ResolvedSemantic> resolved) {
            this.resolved = resolved;
        }

        public ResolveOverall(Collection<ResolvedSemantic> resolved, ResolveStatistics statistics) {
            this.resolved = resolved;
            this.statistics = statistics;
        }

        /**
        * to reduce same object create/drop operations, we first flat and group the candidates by CreateOrDropObject itself,
        * then for each group, if end up with create op in sequence, we will take it as final op. While in opposite,
        * try to find any create op before the last drop op, if no create op found, we will take the last drop op as final op,
        * otherwise, we will assume an transient op as final op.
        * ex. [..., create o1] result in create op
        *     [create o2, drop(if exits) o2, drop(if exists) o2] result in transient op
        *     [drop(if exists) o3, drop(if exists) o3] result in drop op
        * */
        public Collection<CreateOrDropObject> reduceCreateDropObjects() {
            return resolved.stream()
                    .flatMap(r -> r.getCreatedOrDropped().stream())
                    .collect(Collectors.groupingBy(Function.identity(), LinkedHashMap::new, Collectors.toList()))
                    .values().stream().map(list -> {
                        CreateOrDropObject tail = list.get(list.size() - 1);
                        if (tail.getActionType().equals(CreateOrDropObject.ActionType.CREATE)){
                            return tail;
                        }
                        return list.stream().anyMatch(o -> o.getActionType().equals(CreateOrDropObject.ActionType.CREATE)) ? tail.newTransient() : tail;
                    })
                    .collect(Collectors.toList());
        }

        public Collection<ResolvedSemantic> getResolved() {
            return resolved;
        }

        public ResolveStatistics getStatistics() {
            return statistics;
        }

    }

    public static class ResolveStatistics {
        private long elapsedMillis;

        public ResolveStatistics() {
        }

        public ResolveStatistics(long elapsedMillis) {
            this.elapsedMillis = elapsedMillis;
        }

        public long getElapsedMillis() {
            return elapsedMillis;
        }

    }

}
