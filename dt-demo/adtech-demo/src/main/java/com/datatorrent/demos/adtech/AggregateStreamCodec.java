package com.datatorrent.demos.adtech;

import java.io.Serializable;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent;

import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;

public class AggregateStreamCodec extends
  DefaultStatefulStreamCodec<DimensionsEvent.Aggregate> implements Serializable
{

  private static final long serialVersionUID = -8976732832324408655L;

  @Override
  public int getPartition(DimensionsEvent.Aggregate t) {
    // get current aggregator index type from aggIndexPartionTypeMap
    int hash = 5;

    hash = 89 * hash + t.getDimensionDescriptorID();

    return hash;
  }
}
