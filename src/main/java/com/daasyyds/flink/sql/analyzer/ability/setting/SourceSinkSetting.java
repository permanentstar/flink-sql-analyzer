package com.daasyyds.flink.sql.analyzer.ability.setting;

import com.daasyyds.flink.sql.analyzer.semantic.ResolvedSemantic;

import java.net.URI;
import java.util.List;

public class SourceSinkSetting extends ParseSetting {
    private boolean includeCreateObject = true;
    private boolean includeDropObject = true;
    private boolean includeTemporaryObject = true;
    private boolean includeTransient = true;
    private boolean recurseViewToTableSources = false;
    private boolean ignoreNoDatasourceStatement = true;

    /**
    * includeXXX flags will be used in SemanticDatasources, also see {@link ResolvedSemantic.ResolveOverall#reduceCreateDropObjects()}
    * */
    public SourceSinkSetting(List<URI> dependencies, String initCatalog, String initDatabase, boolean includeCreateObject,
                             boolean includeDropObject, boolean includeTemporaryObject, boolean includeTransient,
                             boolean recurseViewToTableSources, boolean ignoreNoDatasourceStatement) {
        super(dependencies, initCatalog, initDatabase);
        this.includeCreateObject = includeCreateObject;
        this.includeDropObject = includeDropObject;
        this.includeTemporaryObject = includeTemporaryObject;
        this.includeTransient = includeTransient;
        this.recurseViewToTableSources = recurseViewToTableSources;
        this.ignoreNoDatasourceStatement = ignoreNoDatasourceStatement;
    }

    public boolean isIncludeCreateObject() {
        return includeCreateObject;
    }

    public boolean isIncludeDropObject() {
        return includeDropObject;
    }

    public boolean isIncludeTemporaryObject() {
        return includeTemporaryObject;
    }

    public boolean isIncludeTransient() {
        return includeTransient;
    }

    public boolean isRecurseViewToTableSources() {
        return recurseViewToTableSources;
    }

    public boolean isIgnoreNoDatasourceStatement() {
        return ignoreNoDatasourceStatement;
    }
}
