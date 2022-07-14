# flink-sql-analyzer
A convenient embedded tool for analyzing flink sql as a client service  
If this repository makes you any benefits on flink sql usage, feel free to star it and recommend to your workmates, hoo ~ daas yyds !!! ^_^

## Background
Recently, the modern industry data development attend to be more platform dependent and high integration with automatic infra devop,
end to end test/prod control on hand, and some other supports like interactive data inspection or meta data extraction.
These demands strongly promote the existing workshop for building data pipelines evolving in time.  
In our practise on [ULTRON](https://mp.weixin.qq.com/s/8cLwKAXPB-A2_a3tmR8-tw) (which is an unified online platform for one-stop data development),
we gradually construct up a service architecture that bridge up the endpoint users and backend bigdata facilities. Here we introduce a service role which
named `sql-analyzer`，shown in the following diagram.  
  
![arch-position](https://user-images.githubusercontent.com/10155248/178899184-484e200f-d6fe-49b6-8b21-febafe37bbda.png)  
  
As described above, for a single sql job, the final goal is to deploy the job graph to remote clusters, which is a heavy and costly operation.
While from the perspective of the entire data pipeline, we may need to concern about data constraint, authority, lineage, relation between datasource/logical tables,
or even simple scene for collecting sql characters online. By this time, a service like sql module will be necessary.
As expected, it should be light weight and client side so can provide an online response as data service, for intermediate info exchange,
for predicate analyze before actually deliver job to backend.
## Functionality
This project does not provide how to establish a web work to communicate with front user, just regard it as a embedded module lib,
at service for your distinguished data services. You can refer the following direction or view the unit test in source for usage.  
If you use maven to build project, just add the current release dependency
```
    
    <!-- madantory denpendencies -->
    <dependency>
      <groupId>com.daasyyds.flink</groupId>
      <artifactId>flink-sql-analyzer</artifactId>
      <version>1.15.1-2022.07.1</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-table-planner_${scala.binary.version}</artifactId>
      <version>1.15.1</version>
    </dependency>

    <!-- optinal dependencies -->
    <!-- add hive dependencies if use hive catalog -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-hive_${scala.binary.version}</artifactId>
      <version>1.15.1</version>
    </dependency>
    <!-- I have tested by 2.3.6, you can use any version supported by flink sql hive connector -->
    <dependency>
      <groupId>org.apache.hive</groupId>
      <artifactId>hive-exec</artifactId>
      <version>${hive.version}</version>
    </dependency>
    <!-- I have tested by 2.7.5, you can use any version supported by flink filesystem -->
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-mapreduce-client-core</artifactId>
      <version>${hadoop.version}</version>
    </dependency>

   <!-- Other denpendency used, for example, kafka connector source and json format usage, add others if needed -->
   <dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-sql-connector-kafka</artifactId>
     <version>1.15.1</version>
   </dependency>

   <dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-json</artifactId>
     <version>1.15.1</version>
   </dependency>
```

### Config entry
Use `CatalogConf` as the config entry, it mainly provide
* the configuration used to construct flink table planner, you can
   * specify an external file path to flink-conf.yml
   * key-value pattern pairs in dynamicConf
   * or by default flink-conf-sql-analyzer.yml in classpath will be used
* catalog relative config
   * external catalog config, as described at [catalog-types](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/catalogs/#catalog-types)
   * load modules, as described at [module-types](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/modules/#module-types)
* setting for planner pool

### Run analyzer functions
Before running any analyze function, create a singleton FlinkAnalyzer first. The easiest way can be done in one line code:
```
    FlinkAnalyzer analyzer = FlinkAnalyzer.newInstance(new CatalogConf());
```
Currently the functions can be categorized into `Parse` interface and `Transform` interface, while the former aims at character collect and the latter
aims at sql rewrite.
#### Multi-sql splitter
Signature: `FlinkAnalyzer.splitStatements(String, ParseSetting)`  
Return: Collection&lt;SingleStatement&gt;  
Category: Parse  
Start Version: 2022.07.1  
Usage: intermediate sql split (Flink sql execution only receive one sql and does not provide any sql splitter)  
Note: the `ParseSetting` is a base class, all flowing settings are inherited from it. In split function, we did not dive into
semantic resolve phase, so init catalog/database and additional jar dependency are not used here. 

#### Sql validator
Signature `FlinkAnalyzer.validate(String, ValidateSetting)`  
Return: true or false(throw exception if `throwIfInvalid` configured to `true`)  
Category: Parse  
Start Version: 2022.07.1  
Usage: intermediate sql validate without push down to cluster side  
Note: we will use table planner to resolve the split sqls, so init catalog and database should be described in ValidateSetting  
(otherwise planner will start to resolve from default catalog/database), if custom udf used, a list of dependencies provided can help to resolve them.  

#### Source & Sink collect for sqls
Signature `FlinkAnalyzer.sourceSinks(String, SourceSinkSetting)`  
Return: Collection&lt;SourceSinks&gt;  
Category: Parse  
Start Version: 2022.07.1  
Usage: intermediate sql analyze on which query sources and modify sinks used in these sqls  
Parameters: `recurseViewToTableSources`, as to query from view, whether recurse the target view to expands as its root tables from, default is false,
which means we expect to find the view and its info in returned list.  
Note: we will simulate sql operations like create/drop table/view/function and context switch op like use catalog/database,
also see `AnalyzerOptions.SEMANTIC_RESOLVE_IGNORE_UNSUPPORTED` and `AnalyzerOptions.SEMANTIC_RESOLVE_UNSUPPORTED_OPERATIONS` for more
details on unsupported operations. The `isXXX` flag for every returned datasource is the `final` state, you can find the explanation at `SourceSinks.Datasource`  

#### Sql explanations
Signature `FlinkAnalyzer.explain(String, ExplainSetting)`  
Return: Collection&lt;ExplainedStatement&gt;
Category: Parse  
Start Version: 2022.07.1  
Usage:  get explain for sqls

#### to be continued..
Transform ability is on the way...