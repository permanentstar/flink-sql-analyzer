package com.daasyyds.flink.sql.analyzer.ability.setting;

import java.net.URI;
import java.util.Collection;
import java.util.List;

public class SqlHintSetting extends ParseSetting {
    private Collection<HintAction> actions;
    private SqlHintSetting(){
    }
    public SqlHintSetting(Collection<HintAction> actions) {
        this.actions = actions;
    }

    public SqlHintSetting(List<URI> dependencies, String initCatalog, String initDatabase, Collection<HintAction> actions) {
        super(dependencies, initCatalog, initDatabase);
        this.actions = actions;
    }

    public Collection<HintAction> getActions() {
        return actions;
    }
}
