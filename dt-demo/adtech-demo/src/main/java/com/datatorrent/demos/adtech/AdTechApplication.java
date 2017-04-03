package com.datatorrent.demos.adtech;

import java.util.ArrayList;
import java.util.Map;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.dimensions.DimensionStoreHDHTNonEmptyQueryResultUnifier;
import com.datatorrent.contrib.enrich.JsonFSLoader;
import com.datatorrent.contrib.enrich.POJOEnricher;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.lib.filter.FilterOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

@ApplicationAnnotation(name = AdTechApplication.APP_NAME)
public class AdTechApplication implements StreamingApplication
{
  public static final String APP_NAME = "adTechDemo";
  public static final String EVENT_SCHEMA = "adsGenericEventSchema.json";
  private String PROP_BASE_PATH_PREFIX = "dt.application." + APP_NAME + ".operator.Store.fileStore.basePathPrefix";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);

    KafkaSinglePortInputOperator kafkaInput = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);
    CsvParser csvParser = dag.addOperator("csvParser", new CsvParser());
    csvParser.setSchema(SchemaUtils.jarResourceFileToString("csvSchema.json"));
    FilterOperator filterLocation = dag.addOperator("filterLocation", FilterOperator.class);

    //setup enricher
    JsonFSLoader fsLoader = new JsonFSLoader();
    POJOEnricher enrich = dag.addOperator("Enrich", POJOEnricher.class);
    enrich.setStore(fsLoader);
    //Sets ExpirationInterval to maximum -> readOnly data never expires
    enrich.setCacheExpirationInterval(Integer.MAX_VALUE);
    ArrayList includeFields = new ArrayList();
    includeFields.add("location");
    ArrayList lookupFields = new ArrayList();
    lookupFields.add("locationID");
    enrich.setIncludeFields(includeFields);
    enrich.setLookupFields(lookupFields);

    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation",
        DimensionsComputationFlexibleSingleSchemaPOJO.class);

    Map<String, String> keyToExpression = Maps.newHashMap();
    keyToExpression.put("publisher", "getPublisher()");
    keyToExpression.put("advertiser", "getAdvertiser()");
    keyToExpression.put("location", "getLocation()");
    keyToExpression.put("time", "getTime()");

    Map<String, String> aggregateToExpression = Maps.newHashMap();
    aggregateToExpression.put("cost", "getCost()");
    aggregateToExpression.put("revenue", "getRevenue()");
    aggregateToExpression.put("impressions", "getImpressions()");
    aggregateToExpression.put("clicks", "getClicks()");

    dimensions.setKeyToExpression(keyToExpression);
    dimensions.setAggregateToExpression(aggregateToExpression);
    dimensions.setConfigurationSchemaJSON(eventSchema);
    dimensions.setUnifier(new DimensionsComputationUnifierImpl<DimensionsEvent.InputEvent, DimensionsEvent.Aggregate>());
    dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(Context.OperatorContext.MEMORY_MB,
      2048);

    //Set store properties
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);
    dag.setOperatorAttribute(store, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    dag.setOperatorAttribute(store, Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);
    AggregateStreamCodec codec = new AggregateStreamCodec();
    dag.setInputPortAttribute(store.input, Context.PortContext.STREAM_CODEC, codec);

    String basePath = Preconditions.checkNotNull(conf.get(PROP_BASE_PATH_PREFIX),
      "Base path prefix should be specified in the properties.xml");
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    hdsFile.setBasePath(basePath + System.currentTimeMillis());
    store.setFileStore(hdsFile);
    store.setConfigurationSchemaJSON(eventSchema);
    store.setEmbeddableQueryInfoProvider(new PubSubWebSocketAppDataQuery());
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());

    //dag.setOutputPortAttribute(dimensions.output, Context.PortContext.UNIFIER_LIMIT, 2);

    store.setQueryResultUnifier(new DimensionStoreHDHTNonEmptyQueryResultUnifier());
    dag.addStream("kafkaInputStream", kafkaInput.outputPort, csvParser.in).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("parsedAdInfoObj", csvParser.out, filterLocation.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filteredStream", filterLocation.truePort, enrich.input);

    ConsoleOutputOperator consoleFiltered = dag.addOperator("consoleFiltered", ConsoleOutputOperator.class);
    dag.addStream("outsorted", filterLocation.falsePort, consoleFiltered.input).setLocality(DAG.Locality.CONTAINER_LOCAL);

    dag.addStream("enrichedStream", enrich.output, dimensions.input);
    dag.addStream("dimensionalData", dimensions.output, store.input);
    dag.addStream("queryResult", store.queryResult, wsOut.input);
  }
}
