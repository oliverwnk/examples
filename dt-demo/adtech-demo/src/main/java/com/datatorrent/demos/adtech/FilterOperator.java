package com.datatorrent.demos.adtech;

import java.util.concurrent.TimeUnit;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by oliver on 2/28/17.
 */
public class FilterOperator extends BaseOperator
{
  public final transient DefaultOutputPort<AdInfo> filteredOutput = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<AdInfo> outSortedOutput = new DefaultOutputPort<>();

  public final transient DefaultInputPort<Object>
    input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object object)
    {
      AdInfo adInfo = (AdInfo)object;
      long hours = TimeUnit.MILLISECONDS.toHours(adInfo.getTime());
      if (17L < hours || hours > 18L) {
        filteredOutput.emit(adInfo);
      } else {
        outSortedOutput.emit(adInfo);
      }
    }
  };

}
