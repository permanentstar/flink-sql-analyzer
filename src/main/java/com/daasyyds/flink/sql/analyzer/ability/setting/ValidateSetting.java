package com.daasyyds.flink.sql.analyzer.ability.setting;

import java.net.URI;
import java.util.List;

public class ValidateSetting extends ParseSetting {
    private boolean throwIfInvalid = true;

    public ValidateSetting(List<URI> dependencies, String initCatalog, String initDatabase, boolean throwIfInvalid) {
        super(dependencies, initCatalog, initDatabase);
        this.throwIfInvalid = throwIfInvalid;
    }

    public boolean isThrowIfInvalid() {
        return throwIfInvalid;
    }

}
