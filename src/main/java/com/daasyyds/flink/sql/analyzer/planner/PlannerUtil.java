package com.daasyyds.flink.sql.analyzer.planner;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ddl.CreateTableASOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

public class PlannerUtil {
    private static final Logger logger = LoggerFactory.getLogger(PlannerUtil.class);

    private static final Field cmTemporaryTables;
    private static final Field fcTempSysFuncs;
    private static final Field fcTempCatalogFuncs;
    private static final Field sinkModifyQuery;
    private static final Field validatorSupplier;
    static {
        try {
            cmTemporaryTables = CatalogManager.class.getDeclaredField("temporaryTables");
            cmTemporaryTables.setAccessible(true);
            fcTempSysFuncs = FunctionCatalog.class.getDeclaredField("tempSystemFunctions");
            fcTempSysFuncs.setAccessible(true);
            fcTempCatalogFuncs = FunctionCatalog.class.getDeclaredField("tempCatalogFunctions");
            fcTempCatalogFuncs.setAccessible(true);
            sinkModifyQuery = CreateTableASOperation.class.getDeclaredField("sinkModifyQuery");
            sinkModifyQuery.setAccessible(true);
            validatorSupplier = ParserImpl.class.getDeclaredField("validatorSupplier");
            validatorSupplier.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("init temporaryTable mapping access for catalog manager failed..", e);
        }
    }


    public static void cleanUpAllTemporaryObject(PlannerBase pb) throws Exception {
        cleanUpTemporaryTableAndViews(pb.catalogManager());
        cleanUpTemporaryFuncs(pb.functionCatalog());
    }

    @SuppressWarnings("unchecked")
    public static void cleanUpTemporaryTableAndViews(CatalogManager cm) throws Exception {
        Map<ObjectIdentifier, CatalogBaseTable> toRemove = (Map<ObjectIdentifier, CatalogBaseTable>) cmTemporaryTables.get(cm);
        Iterator<Map.Entry<ObjectIdentifier, CatalogBaseTable>> itRemove = toRemove.entrySet().iterator();
        while (itRemove.hasNext()) {
            Map.Entry<ObjectIdentifier, CatalogBaseTable> entry = itRemove.next();
            try {
                if (entry.getValue().getTableKind().equals(CatalogBaseTable.TableKind.TABLE)) {
                    cm.dropTemporaryTable(entry.getKey(), true);
                } else {
                    cm.dropTemporaryView(entry.getKey(), true);
                }
            } catch (Exception e) {
                logger.warn("can not drop temporary table/view {}.", entry.getKey().toString());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void cleanUpTemporaryFuncs(FunctionCatalog fc) throws Exception {
        Map<String, CatalogFunction> sysFuncsToRemove = (Map<String, CatalogFunction>) fcTempSysFuncs.get(fc);
        sysFuncsToRemove.keySet().forEach(sysFunc -> {
            try {
                fc.dropTemporarySystemFunction(sysFunc, true);
            } catch (Exception e) {
                logger.warn("drop temporary system function {} fail", sysFunc);
            }
        });
        Map<ObjectIdentifier, CatalogFunction> catalogFuncsToRemove = (Map<ObjectIdentifier, CatalogFunction>) fcTempCatalogFuncs.get(fc);
        catalogFuncsToRemove.keySet().forEach(catalogFunc -> {
            try {
                fc.dropTempCatalogFunction(catalogFunc, true);
            } catch (Exception e) {
                logger.warn("drop temporary catalog function {} fail", catalogFunc);
            }
        });
    }

    public static IdentifierLike catalogObject2Identifier(ObjectIdentifier oi) {
        return IdentifierLike.qualified(oi.asSummaryString());
    }

    public static QueryOperation getSinkModifyQuery(CreateTableASOperation ctas) {
        try {
            return (QueryOperation) sinkModifyQuery.get(ctas);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static FlinkPlannerImpl getFlinkPlanner(Parser parser) {
        try {
            Supplier<FlinkPlannerImpl> supplier = (Supplier<FlinkPlannerImpl>) validatorSupplier.get(parser);
            return supplier.get();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
