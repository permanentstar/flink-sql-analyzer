package com.daasyyds.flink.sql.analyzer.planner;

import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;
import org.apache.flink.util.TemporaryClassLoaderContext;

import com.daasyyds.flink.sql.analyzer.ability.setting.ParseSetting;
import com.daasyyds.flink.sql.analyzer.calcite.DialectParser;
import com.daasyyds.flink.sql.analyzer.common.Promise;
import com.daasyyds.flink.sql.analyzer.config.CatalogConf;
import com.daasyyds.flink.sql.analyzer.planner.FlinkPlannerFactory;
import com.daasyyds.flink.sql.analyzer.rule.Rule;
import com.daasyyds.flink.sql.analyzer.rule.RuleContext;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Supplier;

public class PlannerPool {
    private GenericObjectPool<PlannerBase> plannerPool;
    private CatalogConf.Pool.RuntimeConf runtimeConf;
    private DialectParser dialectParser;

    private PlannerPool(){}

    public PlannerPool(CatalogConf conf) {
        assert conf != null;
        conf.checkValid();
        runtimeConf = conf.getPool().getRuntimeConf();
        initPlannerPool(conf);
    }

    private void initPlannerPool(CatalogConf conf) {
        CatalogConf.Pool poolConf = conf.getPool();
        GenericObjectPoolConfig<PlannerBase> spPollConf = new GenericObjectPoolConfig<>();
        spPollConf.setMaxTotal(poolConf.getMaxTotal());
        spPollConf.setMaxIdle(poolConf.getMaxIdle());
        spPollConf.setMinIdle(poolConf.getMinIdle());
        spPollConf.setTestWhileIdle(poolConf.getTestWhileIdle());
        spPollConf.setTimeBetweenEvictionRuns(poolConf.getSetTimeBetweenEvictionRuns());
        spPollConf.setMinEvictableIdleTime(poolConf.getMinEvictableIdleDuration());
        spPollConf.setJmxEnabled(poolConf.isJmxEnable());
        FlinkPlannerFactory fpf = new FlinkPlannerFactory(conf);
        plannerPool = new GenericObjectPool<>(fpf, spPollConf);
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    if (plannerPool != null) {
                        plannerPool.close();
                    }
                }
        ));
        String dialect = fpf.getConfiguration().get(TableConfigOptions.TABLE_SQL_DIALECT);
        dialectParser = DialectParser.discoverParser(Thread.currentThread().getContextClassLoader(), dialect);
    }

    public <T, R> Promise<T, R> inContext(T input, Rule<T, R> action, ParseSetting setting) {
        PlannerBase pb = getPlanner();
        try {
            if (setting.getInitCatalog() != null) {
                pb.catalogManager().setCurrentCatalog(setting.getInitCatalog());
            }
            if (setting.getInitDatabase() != null) {
                pb.catalogManager().setCurrentDatabase(setting.getInitDatabase());
            }
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            assert loader != null;
            if (setting.getDependencies() != null && !setting.getDependencies().isEmpty()) {
                loader = new URLClassLoader(setting.getDependencies().stream().map(j -> {
                    try {
                        return j.toURL();
                    } catch (MalformedURLException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }).toArray(URL[]::new), loader);
            }
            RuleContext context = SemanticRuleContext.newBuilder()
                    .withPlanner(pb)
                    .withCleanable(() -> returnPlanner(pb))
                    .build();
            return wrapClassLoader(() -> Promise.start(input, action, context), loader);
        } catch (CatalogNotExistException e) {
            returnPlanner(pb);
            throw e;
        }
    }

    private <R> R wrapClassLoader(Supplier<R> supplier, ClassLoader tmpLoader) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(tmpLoader)) {
            return supplier.get();
        }
    }

    private PlannerBase getPlanner() {
        try {
            // this resolved creating/borrowing planner in different threads
            RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE()));
            return plannerPool.borrowObject(runtimeConf.getBorrowMaxWait());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void returnPlanner(PlannerBase planner) {
        plannerPool.returnObject(planner);
        RelMetadataQueryBase.THREAD_PROVIDERS.set(null);
    }

    public DialectParser getDialectParser() {
        return dialectParser;
    }
}
