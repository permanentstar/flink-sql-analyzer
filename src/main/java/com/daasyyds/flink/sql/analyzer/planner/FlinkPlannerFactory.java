package com.daasyyds.flink.sql.analyzer.planner;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.resolver.SqlExpressionResolver;
import org.apache.flink.table.expressions.resolver.lookups.TableReferenceLookup;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.planner.delegation.PlannerBase;

import com.daasyyds.flink.sql.analyzer.config.CatalogConf;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_CATALOG_NAME;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DATABASE_NAME;

public class FlinkPlannerFactory extends BasePooledObjectFactory<PlannerBase> {
    private final Logger logger = LoggerFactory.getLogger(FlinkPlannerFactory.class);

    private Collection<CatalogConf.Catalog> catalogs;
    private Collection<CatalogConf.Module> modules;
    private Configuration configuration;

    private FlinkPlannerFactory() {}

    public FlinkPlannerFactory(CatalogConf catalogConf) {
        this.catalogs = catalogConf.getCatalogs();
        this.modules = catalogConf.getModules();
        Configuration dynamicConf = null;
        if (catalogConf.getDynamicConf() != null) {
            dynamicConf = Configuration.fromMap(catalogConf.getDynamicConf());
        }
        if (catalogConf.getConfDir() != null) {
            configuration = GlobalConfiguration.loadConfiguration(catalogConf.getConfDir());
        } else {
            configuration = CatalogConf.loadDefaultFromClasspath();
        }
        if (dynamicConf != null) {
            configuration.addAll(dynamicConf);
        }
    }

    @Override
    public PlannerBase create() throws Exception {
        StreamExecutionEnvironment see = buildStreamExecutionEnvironment();
        CatalogManager catalogManager = buildCatalogManager(see.getConfig());
        ModuleManager moduleManager = buildModule();
        TableConfig tc = TableConfig.getDefault();
        tc.addConfiguration(configuration);
        FunctionCatalog functionCatalog = new FunctionCatalog(tc, catalogManager, moduleManager);
        Executor executor = AbstractStreamTableEnvironmentImpl.lookupExecutor(Thread.currentThread().getContextClassLoader(), see);
        PlannerBase pb = (PlannerBase)PlannerFactoryUtil.createPlanner(executor, tc, moduleManager, catalogManager, functionCatalog);
        initSchemaResolver(pb);
        return pb;
    }

    private StreamExecutionEnvironment buildStreamExecutionEnvironment() {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        see.getConfig().configure(configuration, Thread.currentThread().getContextClassLoader());
        return see;
    }

    private CatalogManager buildCatalogManager(ExecutionConfig ec) {
        AtomicReference<Catalog> defaultCt = new AtomicReference<>();
        Collection<Catalog> otherCts = new ArrayList<>();
        for (CatalogConf.Catalog c : catalogs) {
            Catalog catalog = FactoryUtil.createCatalog(c.getName(), c.getOptions(), configuration, Thread.currentThread().getContextClassLoader());
            if (c.isUseAsDefault() && defaultCt.getAndSet(catalog) != null) {
                throw new CatalogException("default catalog has been set.");
            } else otherCts.add(catalog);
        }
        if (defaultCt.get() == null) {
            defaultCt.set(new GenericInMemoryCatalog(configuration.get(TABLE_CATALOG_NAME), configuration.get(TABLE_DATABASE_NAME)));
        }
        CatalogManager catalogManager = CatalogManager.newBuilder().classLoader(getClass().getClassLoader())
                .config(configuration)
                .defaultCatalog(((AbstractCatalog)defaultCt.get()).getName(), defaultCt.get())
                .executionConfig(ec)
                .build();
        try {
            defaultCt.get().open();
            for (Catalog c : otherCts) {
                catalogManager.registerCatalog(((AbstractCatalog) c).getName(), c);
            }
        } catch (CatalogException e) {
            unregisterCatalogs(catalogManager);
            throw e;
        }
        return catalogManager;
    }
    private ModuleManager buildModule() {
        ModuleManager moduleManager = new ModuleManager();
        List<String> autoLoadedModules = moduleManager.listModules();
        for (CatalogConf.Module m : modules) {
            if (autoLoadedModules.contains(m.getName())) {
                logger.warn("detect duplicate module name {}, use the custom implement to overwrite builtin one", m.getName());
                moduleManager.unloadModule(m.getName());
            }
            Module module = FactoryUtil.createModule(m.getName(), m.getOptions(), configuration, Thread.currentThread().getContextClassLoader());
            moduleManager.loadModule(m.getName(), module);
        }
        return moduleManager;
    }
    private void initSchemaResolver(PlannerBase pb) {
        boolean isStreamingMode = configuration.get(RUNTIME_MODE) == STREAMING;
        OperationTreeBuilder otb = OperationTreeBuilder.create(
                pb.getTableConfig(),
                pb.functionCatalog().asLookup(pb.getParser()::parseIdentifier),
                pb.catalogManager().getDataTypeFactory(),
                lookupTable(pb.getParser(), pb.catalogManager()),
                resolveExpression(pb.getParser()),
                isStreamingMode);
        pb.catalogManager().initSchemaResolver(isStreamingMode, otb.getResolverBuilder());
    }

    private TableReferenceLookup lookupTable(Parser parser, CatalogManager catalogManager) {
        return path -> {
            try {
                UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(path);
                ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
                Optional<SourceQueryOperation> catalogQueryOperation = catalogManager.getTable(tableIdentifier).map(SourceQueryOperation::new);
                return catalogQueryOperation.map(t -> ApiExpressionUtils.tableRef(path, t));
            } catch (SqlParserException ex) {
                return Optional.empty();
            }
        };
    }

    private SqlExpressionResolver resolveExpression(Parser parser) {
        return (sqlExpression, inputRowType, outputType) -> {
            try {
                return parser.parseSqlExpression(sqlExpression, inputRowType, outputType);
            } catch (Throwable t) {
                throw new ValidationException(String.format("Invalid SQL expression: %s", sqlExpression), t);
            }
        };
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public PooledObject<PlannerBase> wrap(PlannerBase pb) {
        return new DefaultPooledObject<>(pb);
    }

    @Override
    public void destroyObject(PooledObject<PlannerBase> p) {
        CatalogManager cm = p.getObject().catalogManager();
        unregisterCatalogs(cm);
    }

    private void unregisterCatalogs(CatalogManager cm) {
        for (String c : cm.listCatalogs()) {
            try {
                cm.unregisterCatalog(c, true);
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    @Override
    public void passivateObject(PooledObject<PlannerBase> p) throws Exception {
        PlannerUtil.cleanUpAllTemporaryObject(p.getObject());
    }

    @Override
    public boolean validateObject(PooledObject<PlannerBase> p) {
        try {
            CatalogManager cm = p.getObject().catalogManager();
            cm.getCatalog(cm.getBuiltInCatalogName()).ifPresent(Catalog::listDatabases);
        } catch (CatalogException e) {
            return false;
        }
        return true;
    }
}
