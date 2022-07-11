package com.daasyyds.flink.sql.analyzer.planner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.delegation.PlannerBase;

import com.daasyyds.flink.sql.analyzer.rule.RuleContext;

public class SemanticRuleContext implements RuleContext {
    private PlannerBase pb;
    private Configuration configuration;
    private Runnable cleanable;

    private SemanticRuleContext(PlannerBase pb, Configuration configuration, Runnable cleanable) {
        this.pb = pb;
        this.configuration = configuration;
        this.cleanable = cleanable;
    }

    public PlannerBase getPlanner() {
        return pb;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static ContextBuilder newBuilder() {
        return new ContextBuilder();
    }

    @Override
    public void depose() {
        pb = null;
        if (cleanable != null) {
            cleanable.run();
        }
    }

    public static class ContextBuilder {
        private PlannerBase pb;
        private Configuration configuration;
        private Runnable cleanable;

        private ContextBuilder() {
        }

        public ContextBuilder withPlanner(PlannerBase pb) {
            this.pb = pb;
            this.configuration = pb.getTableConfig().getConfiguration();
            return this;
        }

        public ContextBuilder withCleanable(Runnable cleanable) {
            this.cleanable = cleanable;
            return this;
        }

        public SemanticRuleContext build() {
            return new SemanticRuleContext(pb, configuration, cleanable);
        }
    }
}
