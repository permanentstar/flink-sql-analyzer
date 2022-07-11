package com.daasyyds.flink.sql.analyzer.ability.result;

import com.daasyyds.flink.sql.analyzer.semantic.ResolvedSemantic;

import java.util.Collection;
import java.util.Objects;

public class SourceSinks extends SingleStatement {
    private Collection<Datasource> datasources;

    public SourceSinks() {
    }

    public SourceSinks(String sqlType, String sql, int sqlIndex, Collection<Datasource> datasources) {
        super(sqlType, sql, sqlIndex);
        this.datasources = datasources;
    }

    public Collection<Datasource> getDatasources() {
        return datasources;
    }

    public void setDatasources(Collection<Datasource> datasources) {
        this.datasources = datasources;
    }

    /**
     * isXXX flags indicate the final state for current Datasource, also see {@link ResolvedSemantic.ResolveOverall#reduceCreateDropObjects()}
     * */
    public static class Datasource extends IdentifierLike {
        private boolean isCreated;
        private boolean isDropped;
        private boolean isTemporary;
        private boolean isTransient;
        private boolean isView;
        private boolean isSink;

        public Datasource(String catalog, String database, String name) {
            super(catalog, database, name);
        }

        public Datasource(String catalog, String database, String name, boolean isCreated, boolean isDropped,
                          boolean isTemporary, boolean isTransient, boolean isView, boolean isSink) {
            super(catalog, database, name);
            this.isCreated = isCreated;
            this.isDropped = isDropped;
            this.isTemporary = isTemporary;
            this.isTransient = isTransient;
            this.isView = isView;
            this.isSink = isSink;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            Datasource that = (Datasource) o;
            return isCreated == that.isCreated &&
                    isDropped == that.isDropped &&
                    isTemporary == that.isTemporary &&
                    isTransient == that.isTransient &&
                    isView == that.isView &&
                    isSink == that.isSink;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), isCreated, isDropped, isTemporary, isView, isSink);
        }

        public boolean isExternalCatalogDatasource() {
            return !isTemporary;
        }

        public boolean isCreated() {
            return isCreated;
        }

        public void setCreated(boolean created) {
            isCreated = created;
        }

        public boolean isDropped() {
            return isDropped;
        }

        public void setDropped(boolean dropped) {
            isDropped = dropped;
        }

        public boolean isTemporary() {
            return isTemporary;
        }

        public void setTemporary(boolean temporary) {
            isTemporary = temporary;
        }

        public boolean isTransient() {
            return isTransient;
        }

        public void setTransient(boolean aTransient) {
            isTransient = aTransient;
        }

        public boolean isView() {
            return isView;
        }

        public void setView(boolean view) {
            isView = view;
        }

        public boolean isSink() {
            return isSink;
        }

        public void setSink(boolean sink) {
            isSink = sink;
        }
    }
}
