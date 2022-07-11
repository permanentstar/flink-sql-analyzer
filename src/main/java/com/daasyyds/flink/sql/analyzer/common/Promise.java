package com.daasyyds.flink.sql.analyzer.common;

import com.daasyyds.flink.sql.analyzer.rule.CollectRule;
import com.daasyyds.flink.sql.analyzer.rule.FlatMapRule;
import com.daasyyds.flink.sql.analyzer.rule.Rule;
import com.daasyyds.flink.sql.analyzer.rule.RuleContext;
import com.daasyyds.flink.sql.analyzer.rule.SplitMergeRule;

import java.util.Collection;
import java.util.function.Supplier;

/**
* working like a periodic rule execution, that means after {@link #realize()}, the fired result can be input of next Promise and fired rule should never be used again,
* call {@link #then(Rule, Supplier)} to chain up next Promise, a new {@link RuleContext} can replace the old one(witch should be deposed first).
* */
public abstract class Promise<T, R> {
    Rule<T, R> activity;
    RuleContext context;
    R result;
    boolean resolved;

    private Promise() {
    }

    private Promise(Rule<T, R> activity, RuleContext context) {
        this.activity = activity;
        this.context = context;
    }

    public abstract R realize();

    public R realizeAndResolve() {
        if (result == null) {
            result = realize();
        }
        resolve();
        return result;
    }

    private void resolve() {
        if (!resolved) {
            context.depose();
            resolved = true;
        }
    }

    public boolean isResolved() {
        return resolved;
    }

    public R getResult() {
        return result;
    }

    public <S, U> Promise<R, Collection<U>> split(FlatMapRule<R, S> splitRule, Supplier<Rule<S, U>> transformRuleProvider) {
        realizeAndCheckResultNonNull();
        R nextInput = result;
        SplitMergeRule<R, S, U, Collection<U>> smr = SplitMergeRule.of(splitRule, transformRuleProvider.get(), CollectRule.identity());
        return new Promise<R, Collection<U>>(smr, context) {
            @Override
            public Collection<U> realize() {
                this.result = this.activity.apply(nextInput, this.context);
                return this.result;
            }
        };
    }

    public <S> Promise<R, S> then(Rule<R, S> activity, Supplier<RuleContext> contextProvider) {
        RuleContext newContext = contextProvider.get();
        boolean inherit = newContext == RuleContext.INHERIT_CONTEXT;
        if (resolved && inherit) {
            throw new RuntimeException("the inherited context has been deposed, action denied");
        } else if (!inherit && !resolved) {
            throw new RuntimeException("to avoid resource leak, previous context should be deposed before apply new context, action denied");
        } else if (inherit) {
            realizeAndCheckResultNonNull();
        }
        R nextInput = result;
        return new Promise<R, S>(activity, inherit ? context : newContext) {
            @Override
            public S realize() {
                this.result = this.activity.apply(nextInput, this.context);
                return this.result;
            }
        };
    }

    private void realizeAndCheckResultNonNull() {
        if (result == null) {
            result = realize();
            if (result == null) {
                throw new RuntimeException("current promise has not been realized successfully, result is still null, action denied");
            }
        }
    }

    public <S> Promise<R, S> then(Rule<R, S> activity) {
        return then(activity, RuleContext::inheritContext);
    }

    public static <T, R> Promise<T, R> start(T input, Rule<T, R> activity, RuleContext context) {
        return new PromiseHead<>(input, activity, context);
    }

    private static class PromiseHead<T, R> extends Promise<T, R> {
        private T input;

        private PromiseHead(T input, Rule<T, R> activity, RuleContext context) {
            super(activity, context);
            this.input = input;
        }

        @Override
        public R realize() {
            result = activity.apply(input, context);
            return result;
        }
    }

}
