package com.daasyyds.flink.sql.analyzer.ability.result;

import com.daasyyds.flink.sql.analyzer.common.AnalyzerException;
import com.daasyyds.flink.sql.analyzer.common.StringUtil;

import java.util.Objects;

public class IdentifierLike {
    private String catalog;
    private String database;
    private String name;

    public IdentifierLike() {
    }

    protected IdentifierLike(String catalog, String database, String name) {
        this.catalog = catalog;
        this.database = database;
        this.name = name;
    }

    public static IdentifierLike of(String catalog, String database, String name) {
        return new IdentifierLike(catalog, database, name);
    }

    public static IdentifierLike qualified(String objectPath) {
        String[] paths = objectPath.split("\\.", -1);
        if (paths.length == 1) {
            return ofAnonymous(paths[0]);
        } else if (paths.length == 2) {
            return of(null, paths[0], paths[1]);
        } else if (paths.length == 3) {
            return of(paths[0], paths[1], paths[2]);
        } else throw new AnalyzerException(String.format("unexpect qualified path %s", objectPath));
    }

    public static IdentifierLike ofAnonymous(String name) {
        assert name != null;
        return new IdentifierLike(null, null, name);
    }

    public static IdentifierLike ofCatalog(String catalog) {
        assert catalog != null;
        return new IdentifierLike(catalog, null, null);
    }

    public static IdentifierLike ofDatabase(String catalog, String database) {
        assert catalog != null && database != null;
        return new IdentifierLike(catalog, database, null);
    }

    public static String fullQualifier(IdentifierLike il) {
        StringBuilder sb = new StringBuilder();
        if (StringUtil.notEmpty(il.catalog)) {
            sb.append(il.catalog).append(".");
        }
        if (StringUtil.notEmpty(il.database)) {
            sb.append(il.database).append(".");
        }
        if (StringUtil.notEmpty(il.name)) {
            sb.append(il.name);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IdentifierLike that = (IdentifierLike) o;
        return Objects.equals(catalog, that.catalog) &&
                Objects.equals(database, that.database) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, database, name);
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
