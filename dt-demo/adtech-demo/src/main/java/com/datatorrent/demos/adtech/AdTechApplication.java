package com.datatorrent.demos.adtech;

import java.util.ArrayList;
import java.util.Map;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
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

@ApplicationAnnotation(name = "adTechDemo")
public class AdTechApplication implements StreamingApplication
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
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);

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

    //Set store properties
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    hdsFile.setBasePath("adTechDataStorePath");
    store.setFileStore(hdsFile);
    store.setConfigurationSchemaJSON(eventSchema);

    store.setEmbeddableQueryInfoProvider(new PubSubWebSocketAppDataQuery());
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());

    dag.addStream("kafkaInputStream", kafkaInput.outputPort, csvParser.in).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("parsedAdInfoObj", csvParser.out, filterLocation.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filteredStream", filterLocation.truePort, enrich.input).setLocality(DAG.Locality.CONTAINER_LOCAL);

    ConsoleOutputOperator consoleFiltered = dag.addOperator("consoleFiltered", ConsoleOutputOperator.class);
    dag.addStream("outsorted", filterLocation.falsePort, consoleFiltered.input).setLocality(DAG.Locality.CONTAINER_LOCAL);

    dag.addStream("InputStream", enrich.output, dimensions.input);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input);
  }
}
