package com.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by oliver on 2/1/17.
 */
public class WindowIdInputOperator extends BaseOperator implements InputOperator
{
  int count;
  long windowId;
  boolean emit;

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  public void emitTuples()
  {
    if (count > 0) {
      output.emit(String.valueOf(windowId));
      windowId++;
    }
  }

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {
    count--;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    count = 50;
    windowId = 1;
  }

  @Override
  public void teardown()
  {

  }
}
