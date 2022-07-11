package com.daasyyds.flink.sql.analyzer.ability.setting;

import java.net.URI;
import java.util.List;

public class ExplainSetting extends ParseSetting {
    private boolean ignoreNoExplainable = true;

    public ExplainSetting() {
    }

    public ExplainSetting(List<URI> dependencies, String initCatalog, String initDatabase, boolean ignoreNoExplainable) {
        super(dependencies, initCatalog, initDatabase);
        this.ignoreNoExplainable = ignoreNoExplainable;
    }

    public boolean isIgnoreNoExplainable() {
        return ignoreNoExplainable;
    }

}
