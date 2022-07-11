package com.daasyyds.flink.sql.analyzer.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.module.ModuleException;

import com.daasyyds.flink.sql.analyzer.common.CollectionUtil;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class CatalogConf {
    public static final String ANALYZER_CONF_FILE = "flink-conf-sql-analyzer.yaml";
    private Pool pool;
    private String confDir;
    private Map<String, String> dynamicConf;
    private Collection<Catalog> catalogs;
    private Collection<Module> modules;

    public CatalogConf() {
        this(Collections.emptyList(), Collections.emptyList());
    }

    public CatalogConf(Collection<Catalog> catalogs, Collection<Module> modules) {
        this(Collections.emptyMap(), catalogs, modules);
    }

    public CatalogConf(Map<String, String> dynamicConf, Collection<Catalog> catalogs, Collection<Module> modules) {
        this(Pool.getDefault(), null, dynamicConf, catalogs, modules);
    }

    public CatalogConf(Pool pool, String confDir, Map<String, String> dynamicConf, Collection<Catalog> catalogs, Collection<Module> modules) {
        this.pool = pool;
        this.confDir = confDir;
        this.dynamicConf = dynamicConf;
        this.catalogs = catalogs;
        this.modules = modules;
    }

    public void checkValid() {
        assert pool != null;
        if (catalogs != null && catalogs.size() > 0) {
            if (catalogs.stream().map(Catalog::getName).count() != catalogs.size()) {
                throw new CatalogException("catalogs definition should distinct names");
            }
            long defaultCnt = catalogs.stream().filter(Catalog::isUseAsDefault).count();
            if (defaultCnt > 1) {
                throw new CatalogException("only one default catalog can be specified.");
            }
        }
        if (modules != null && modules.size() > 0) {
            if (modules.stream().map(Module::getName).count() != modules.size()) {
                throw new ModuleException("module definition should distinct names");
            }
        }
    }

    public static Configuration loadDefaultFromClasspath() {
        Configuration config = new Configuration();
        try(InputStream is = CatalogConf.class.getClassLoader().getResourceAsStream(ANALYZER_CONF_FILE)) {
            Map<String, Object> map = new Yaml().load(is);
            map = CollectionUtil.parseNestedMapToDotPathMap(map);
            if (!map.isEmpty()) {
                map.forEach((k,v) -> {
                    Optional<AnalyzerOptions.WrappedConfigOption<?>> opt = AnalyzerOptions.preDefined(k);
                    if (opt.isPresent()) {
                        opt.get().setConfiguration(config, v);
                    } else {
                        config.setString(k.trim(), v.toString().trim());
                    }
                });
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing default YAML configuration.", e);
        }
        return config;
    }

    @Override
    public String toString() {
        return "CatalogConf{" +
                "pool=" + pool +
                ", confDir='" + confDir + '\'' +
                ", dynamicConf=" + dynamicConf +
                ", catalogs=" + catalogs +
                ", modules=" + modules +
                '}';
    }

    public Pool getPool() {
        return pool;
    }

    public void setPool(Pool pool) {
        this.pool = pool;
    }

    public String getConfDir() {
        return confDir;
    }

    public void setConfDir(String confDir) {
        this.confDir = confDir;
    }

    public Map<String, String> getDynamicConf() {
        return dynamicConf;
    }

    public void setDynamicConf(Map<String, String> dynamicConf) {
        this.dynamicConf = dynamicConf;
    }

    public Collection<Catalog> getCatalogs() {
        return catalogs;
    }

    public void setCatalogs(Collection<Catalog> catalogs) {
        this.catalogs = catalogs;
    }

    public Collection<Module> getModules() {
        return modules;
    }

    public void setModules(Collection<Module> modules) {
        this.modules = modules;
    }

    public static final class Catalog {
        private String name;
        private boolean useAsDefault;
        private Map<String, String> options;

        private Catalog() {}

        public Catalog(String name, Map<String, String> options) {
            this.name = name;
            this.options = options;
        }

        public Catalog(String name, boolean useAsDefault, Map<String, String> options) {
            this.name = name;
            this.useAsDefault = useAsDefault;
            this.options = options;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public boolean isUseAsDefault() {
            return useAsDefault;
        }

        public void setUseAsDefault(boolean useAsDefault) {
            this.useAsDefault = useAsDefault;
        }

        public Map<String, String> getOptions() {
            return options;
        }

        public void setOptions(Map<String, String> options) {
            this.options = options;
        }

        @Override
        public String toString() {
            return "Catalog{" +
                    "name='" + name + '\'' +
                    ", useAsDefault=" + useAsDefault +
                    ", options=" + options +
                    '}';
        }
    }

    public static final class Module {
        private String name;
        private Map<String, String> options = Collections.emptyMap();

        private Module() {}

        public Module(String name) {
            this.name = name;
        }

        public Module(String name, Map<String, String> options) {
            this.name = name;
            this.options = options;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Map<String, String> getOptions() {
            return options;
        }

        public void setOptions(Map<String, String> options) {
            this.options = options;
        }

        @Override
        public String toString() {
            return "Module{" +
                    "name='" + name + '\'' +
                    ", options=" + options +
                    '}';
        }
    }

    public static final class Pool {
        private static final Pool DEFAULT_POOL_CONF = new Pool();

        private Integer maxTotal = 8;
        private Integer maxIdle = 6;
        private Integer minIdle = 2;
        private Boolean testWhileIdle = true;
        private Duration setTimeBetweenEvictionRuns = Duration.ofMinutes(5);
        private Duration minEvictableIdleDuration = Duration.ofMinutes(1);
        private RuntimeConf runtimeConf;

        private boolean jmxEnable;

        public static Pool getDefault() {
            return DEFAULT_POOL_CONF;
        }

        private Pool() {
            this.runtimeConf = new RuntimeConf();
        }

        public Pool(Integer maxTotal, Integer maxIdle, Integer minIdle, Boolean testWhileIdle, Duration setTimeBetweenEvictionRuns, Long borrowMaxWait) {
            this.maxTotal = maxTotal;
            this.maxIdle = maxIdle;
            this.minIdle = minIdle;
            this.testWhileIdle = testWhileIdle;
            this.setTimeBetweenEvictionRuns = setTimeBetweenEvictionRuns;
            this.runtimeConf = new RuntimeConf(borrowMaxWait);
        }

        public Integer getMaxTotal() {
            return maxTotal;
        }

        public void setMaxTotal(Integer maxTotal) {
            this.maxTotal = maxTotal;
        }

        public Integer getMaxIdle() {
            return maxIdle;
        }

        public void setMaxIdle(Integer maxIdle) {
            this.maxIdle = maxIdle;
        }

        public Integer getMinIdle() {
            return minIdle;
        }

        public void setMinIdle(Integer minIdle) {
            this.minIdle = minIdle;
        }

        public Boolean getTestWhileIdle() {
            return testWhileIdle;
        }

        public void setTestWhileIdle(Boolean testWhileIdle) {
            this.testWhileIdle = testWhileIdle;
        }

        public Duration getSetTimeBetweenEvictionRuns() {
            return setTimeBetweenEvictionRuns;
        }

        public void setSetTimeBetweenEvictionRuns(Duration setTimeBetweenEvictionRuns) {
            this.setTimeBetweenEvictionRuns = setTimeBetweenEvictionRuns;
        }

        public Duration getMinEvictableIdleDuration() {
            return minEvictableIdleDuration;
        }

        public void setMinEvictableIdleDuration(Duration minEvictableIdleDuration) {
            this.minEvictableIdleDuration = minEvictableIdleDuration;
        }

        public RuntimeConf getRuntimeConf() {
            return runtimeConf;
        }

        public void setRuntimeConf(RuntimeConf runtimeConf) {
            this.runtimeConf = runtimeConf;
        }

        public boolean isJmxEnable() {
            return jmxEnable;
        }

        public void setJmxEnable(boolean jmxEnable) {
            this.jmxEnable = jmxEnable;
        }

        @Override
        public String toString() {
            return "Pool{" +
                    "maxTotal=" + maxTotal +
                    ", maxIdle=" + maxIdle +
                    ", minIdle=" + minIdle +
                    ", testWhileIdle=" + testWhileIdle +
                    ", setTimeBetweenEvictionRuns=" + setTimeBetweenEvictionRuns +
                    ", minEvictableIdleDuration=" + minEvictableIdleDuration +
                    ", runtimeConf=" + runtimeConf +
                    ", jmxEnable=" + jmxEnable +
                    '}';
        }

        public static class RuntimeConf {
            private Long borrowMaxWait = 60_000L;

            public RuntimeConf() {
            }

            public RuntimeConf(Long borrowMaxWait) {
                this.borrowMaxWait = borrowMaxWait;
            }

            public Long getBorrowMaxWait() {
                return borrowMaxWait;
            }

            public void setBorrowMaxWait(Long borrowMaxWait) {
                this.borrowMaxWait = borrowMaxWait;
            }

            @Override
            public String toString() {
                return "RuntimeConf{" +
                        "borrowMaxWait=" + borrowMaxWait +
                        '}';
            }
        }
    }
}
