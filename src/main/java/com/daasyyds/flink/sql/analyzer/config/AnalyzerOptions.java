package com.daasyyds.flink.sql.analyzer.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.operations.Operation;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnalyzerOptions {
    public static final ConfigOption<Boolean> SEMANTIC_RESOLVE_IGNORE_UNSUPPORTED =
            ConfigOptions.key("analyzer.semantic.resolve.unsupported.ignore")
            .booleanType()
            .defaultValue(true)
            .withDescription("In semantic resolve phase, whether skip resolve on unsupported operation. " +
                    "For example: we can not simulate a create database operation, if this value set to false" +
                    ", an UnsupportedOperationException will be thrown out.");
    public static final ConfigOption<List<String>> SEMANTIC_RESOLVE_UNSUPPORTED_OPERATIONS =
            ConfigOptions.key("analyzer.semantic.resolve.unsupported.operations")
            .stringType()
            .asList()
            .defaultValues(
                    "org.apache.flink.table.operations.ddl.CreateCatalogOperation",
                    "org.apache.flink.table.operations.ddl.DropCatalogOperation",
                    "org.apache.flink.table.operations.ddl.CreateDatabaseOperation",
                    "org.apache.flink.table.operations.ddl.DropDatabaseOperation",
                    "org.apache.flink.table.operations.ddl.AlterOperation",
                    "org.apache.flink.table.operations.command.AddJarOperation",
                    "org.apache.flink.table.operations.command.ClearOperation",
                    "org.apache.flink.table.operations.command.RemoveJarOperation",
                    "org.apache.flink.table.operations.command.SetOperation",
                    "org.apache.flink.table.operations.ResetOperation",
                    "org.apache.flink.table.operations.LoadModuleOperation",
                    "org.apache.flink.table.operations.UnloadModuleOperation"
            )
            .withDescription("In semantic resolve phase, the unsupported operations.");

    private static List<Class<? extends Operation>> unsupportedOperations;
    public static boolean isSemanticResolveSupported(Class<? extends Operation> opClz, Configuration configuration) {
        if (unsupportedOperations == null) {
            synchronized(AnalyzerOptions.class) {
                List<String> unsupportedList  = configuration.get(SEMANTIC_RESOLVE_UNSUPPORTED_OPERATIONS);

                unsupportedOperations = unsupportedList.stream()
                        .map(c -> {
                            try {
                                Class<?> clz = AnalyzerOptions.class.getClassLoader().loadClass(c);
                                return (Class<? extends Operation>)clz.asSubclass(Operation.class);
                            } catch (ClassNotFoundException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .collect(Collectors.toList());
            }
        }
        return unsupportedOperations.stream().noneMatch(uo -> uo.isAssignableFrom(opClz));
    }

    public static Optional<WrappedConfigOption<?>> preDefined(String key) {
        return Stream.of(
                SEMANTIC_RESOLVE_IGNORE_UNSUPPORTED,
                SEMANTIC_RESOLVE_UNSUPPORTED_OPERATIONS)
                .filter(o -> o.key().equals(key))
                .findFirst()
                .map(WrappedConfigOption::of);
    }

    public static class WrappedConfigOption<T> {
        private ConfigOption<T> option;

        private WrappedConfigOption(ConfigOption<T> option) {
            this.option = option;
        }

        public static <T> WrappedConfigOption<T> of(ConfigOption<T> option) {
            return new WrappedConfigOption<>(option);
        }

        @SuppressWarnings("unchecked")
        public T cast(Object value) {
            return (T) value;
        }

        public ConfigOption<T> getOption() {
            return option;
        }

        public void setConfiguration(Configuration configuration, Object value) {
            configuration.set(option, cast(value));
        }
    }
}
