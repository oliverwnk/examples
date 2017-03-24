package com.datatorrent.demos.adtech;

import java.net.URI;
import java.util.ArrayList;
import java.util.Map;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.viewfs.ConfigUtil;

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
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.lib.filter.FilterOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

@ApplicationAnnotation(name = "adTechDemo")
public class  AdTechApplication implements StreamingApplication
{
  public static final String EVENT_SCHEMA = "adsGenericEventSchema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);

    KafkaSinglePortInputOperator kafkaInput = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);
    CsvParser csvParser = dag.addOperator("csvParser", new CsvParser());
    csvParser.setSchema(SchemaUtils.jarResourceFileToString("csvSchema.json"));
    FilterOperator filterLocation = dag.addOperator("filterLocation", FilterOperator.class);

    JsonFSLoader fsLoader = new JsonFSLoader();
    POJOEnricher enrich = dag.addOperator("Enrich", POJOEnricher.class);
    enrich.setStore(fsLoader);

    ArrayList includeFields = new ArrayList();
    includeFields.add("location");
    ArrayList lookupFields = new ArrayList();
    lookupFields.add("locationID");
    enrich.setIncludeFields(includeFields);
    enrich.setLookupFields(lookupFields);

    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation", DimensionsComputationFlexibleSingleSchemaPOJO.class);


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
        8092);

    //Set store properties
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);
    dag.setOperatorAttribute(store, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    dag.setOperatorAttribute(store, Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);

    //store.setUpdateEnumValues(true);
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    hdsFile.setBasePath("AdsDimensionsDemo/Store" + System.currentTimeMillis());
    store.setFileStore(hdsFile);
    //dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
    //  new BasicCounters.LongAggregator<MutableLong>());
    store.setConfigurationSchemaJSON(eventSchema);

    store.setNumberOfBuckets(16);
    store.setPartitionCount(16);

    //PubSubWebSocketAppDataQuery query = new PubSubWebSocketAppDataQuery();
    //URI queryUri = ConfigUtil.getAppDataQueryPubSubURI(dag, conf);
    //query.setUri(queryUri);
    store.setEmbeddableQueryInfoProvider(new PubSubWebSocketAppDataQuery());
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());



    store.setQueryResultUnifier(new DimensionStoreHDHTNonEmptyQueryResultUnifier());
    dag.addStream("kafkaInputStream", kafkaInput.outputPort, csvParser.in).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("parsedAdInfoObj", csvParser.out, filterLocation.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filteredStream", filterLocation.truePort, enrich.input);

    ConsoleOutputOperator consoleFiltered = dag.addOperator("consoleFiltered", ConsoleOutputOperator.class);
    dag.addStream("outsorted", filterLocation.falsePort, consoleFiltered.input).setLocality(DAG.Locality.CONTAINER_LOCAL);

    dag.addStream("InputStream", enrich.output, dimensions.input);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input);
  }
}
