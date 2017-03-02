package com.datatorrent.demos.adtech;

import java.util.List;

import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;

public class EnricherOperator extends BaseOperator
{
  private transient DimensionalConfigurationSchema schema;
  public List<Object> locationNames;
  public List<Object> publisherNames;
  public List<Object> advertiserNames;

  public final transient DefaultOutputPort<AdInfo> output = new DefaultOutputPort<>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
    //get list of locations from csvSchema.json to enrich publisher, advertiser and location
    String schemaJson = SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json");
    schema = new DimensionalConfigurationSchema(schemaJson, AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
    publisherNames = schema.getKeysToEnumValuesList().get("publisher");
    advertiserNames = schema.getKeysToEnumValuesList().get("advertiser");
    locationNames = schema.getKeysToEnumValuesList().get("location");

  }

  public final transient DefaultInputPort<AdInfo>
    input = new DefaultInputPort<AdInfo>()
  {

    @Override
    public void process(AdInfo adInfo)
    {
      String publisher = (String)publisherNames.get(adInfo.getPublisherID());
      String advertiser = (String)advertiserNames.get(adInfo.getAdvertiserID());
      String location = (String)locationNames.get(adInfo.getLocationID());

      adInfo.setPublisher(publisher);
      adInfo.setAdvertiser(advertiser);
      adInfo.setLocation(location);

      output.emit(adInfo);
    }
  };
}
