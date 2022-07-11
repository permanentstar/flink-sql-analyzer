package com.daasyyds.flink.sql.analyzer.ability.setting.hint;

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;

public interface TableAware {
    IdentifierLike aware();
}
