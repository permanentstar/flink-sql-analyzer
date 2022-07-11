package com.daasyyds.flink.sql.analyzer.ability.setting;

import java.net.URI;
import java.util.List;

public class ParseSetting {
    private List<URI> dependencies;
    private String initCatalog;
    private String initDatabase;

    public ParseSetting() {
    }

    public ParseSetting(List<URI> dependencies, String initCatalog, String initDatabase) {
        this.dependencies = dependencies;
        this.initCatalog = initCatalog;
        this.initDatabase = initDatabase;
    }

    public List<URI> getDependencies() {
        return dependencies;
    }

    public String getInitCatalog() {
        return initCatalog;
    }

    public String getInitDatabase() {
        return initDatabase;
    }

}
