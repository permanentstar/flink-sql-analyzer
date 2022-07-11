package com.daasyyds.flink.sql.analyzer.ability;

import com.daasyyds.flink.sql.analyzer.ability.result.ExplainedStatement;
import com.daasyyds.flink.sql.analyzer.ability.result.SingleStatement;
import com.daasyyds.flink.sql.analyzer.ability.result.SourceSinks;
import com.daasyyds.flink.sql.analyzer.ability.setting.ExplainSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.ParseSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.SourceSinkSetting;
import com.daasyyds.flink.sql.analyzer.ability.setting.ValidateSetting;

import java.util.Collection;

public interface Parse {
    boolean validate(String sqls, ValidateSetting setting);
    Collection<SingleStatement> splitStatements(String sqls, ParseSetting setting);
    Collection<SourceSinks> sourceSinks(String sqls, SourceSinkSetting setting);
    Collection<ExplainedStatement> explain(String sqls, ExplainSetting setting);
}
