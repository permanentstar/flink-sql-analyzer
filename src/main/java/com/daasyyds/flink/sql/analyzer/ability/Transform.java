package com.daasyyds.flink.sql.analyzer.ability;

import com.daasyyds.flink.sql.analyzer.ability.result.TranslatedStatement;
import com.daasyyds.flink.sql.analyzer.ability.setting.ReplaceTableSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.SqlHintSetting;

import java.util.Collection;

public interface Transform {
    Collection<TranslatedStatement> applyHints(String sqls, SqlHintSetting setting);
    Collection<TranslatedStatement> applyReplaceTables(String sqls, ReplaceTableSetting setting);
}
