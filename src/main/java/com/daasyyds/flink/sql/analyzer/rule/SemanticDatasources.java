package com.daasyyds.flink.sql.analyzer.rule;

import org.apache.flink.types.Either;

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;
import com.daasyyds.flink.sql.analyzer.ability.result.SourceSinks;
import com.daasyyds.flink.sql.analyzer.ability.setting.SourceSinkSetting;
import com.daasyyds.flink.sql.analyzer.semantic.CreateOrDropObject;
import com.daasyyds.flink.sql.analyzer.semantic.ResolvedSemantic;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class SemanticDatasources implements Rule<ResolvedSemantic, SourceSinks> {
    private Collection<CreateOrDropObject> cds;
    private boolean includeCreateObject;
    private boolean includeDropObject;
    private boolean includeTemporaryObject;
    private boolean includeTransient;

    private SemanticDatasources(Collection<CreateOrDropObject> cds, boolean includeCreateObject,
                                boolean includeDropObject, boolean includeTemporaryObject, boolean includeTransient) {
        this.cds = cds;
        this.includeCreateObject = includeCreateObject;
        this.includeDropObject = includeDropObject;
        this.includeTemporaryObject = includeTemporaryObject;
        this.includeTransient = includeTransient;
    }

    public static SemanticDatasources of(Collection<CreateOrDropObject> cds, SourceSinkSetting setting) {
        return new SemanticDatasources(cds, setting.isIncludeCreateObject(), setting.isIncludeDropObject(),
                setting.isIncludeTemporaryObject(), setting.isIncludeTransient());
    }

    @Override
    public SourceSinks apply(ResolvedSemantic rs, RuleContext context) {
        Collection<SourceSinks.Datasource> ssos = rs.getSourceOrSink()
                .stream()
                .map(o -> resolveSourceSink(o.getIdentifier(), o.isTemporary(), o.isView(), o.isSink()).orElse(null))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return new SourceSinks(rs.getSqlType(), rs.getSql(), rs.getSqlIndex(), ssos);
    }

    private Optional<SourceSinks.Datasource> resolveSourceSink(IdentifierLike il, boolean isTemporary, boolean isView, boolean isSink) {
        Either<Boolean, Optional<CreateOrDropObject>> lor = matchCreateOrDropObject(il);
        SourceSinks.Datasource sso = null;
        if (lor.isLeft()) {
            sso = new SourceSinks.Datasource(il.getCatalog(), il.getDatabase(), il.getName(), false,false,
                    isTemporary, false, isView, isSink);
        } else if (lor.right().isPresent()) {
            CreateOrDropObject coo = lor.right().get();
            sso = new SourceSinks.Datasource(il.getCatalog(), il.getDatabase(), il.getName(), coo.isCreated(), coo.isDropped(),
                    coo.isTemporary(), coo.isTransient(), coo.getObjectType().equals(CreateOrDropObject.ObjectType.VIEW), isSink);
        }
        return Optional.ofNullable(sso);
    }

    private Either<Boolean, Optional<CreateOrDropObject>> matchCreateOrDropObject(IdentifierLike il) {
        Optional<CreateOrDropObject> opt = cds.stream().filter(o -> o.matchObject(il)).findFirst();
        if (opt.isPresent()) {
            CreateOrDropObject cdo = opt.get();
            if (!includeCreateObject && cdo.isCreated() ||
                    !includeDropObject && cdo.isDropped() ||
                    !includeTransient && cdo.isTransient() ||
                    !includeTemporaryObject && cdo.isTemporary()) {
                opt = Optional.empty();
            }
            return Either.Right(opt);
        }
        return Either.Left(false);
    }
}
