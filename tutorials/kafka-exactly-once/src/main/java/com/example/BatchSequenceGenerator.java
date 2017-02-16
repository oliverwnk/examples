package com.example;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Min;

/**
 * Simple operator that emits Strings from 1 to maxTuplesTotal
 */
public class BatchSequenceGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(BatchSequenceGenerator.class);

  // properties

  @Min(1)
  private int maxTuplesTotal;     // max number of tuples in total
  @Min(1)
  private int maxTuples;           // max number of tuples per window

  private int sleepTime;

  private int numTuplesTotal = 0;

  // transient fields

  private transient int numTuples = 0;    // number emitted in current window

  public final transient DefaultOutputPort<String> out = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    numTuples = 0;
    super.beginWindow(windowId);
  }

  @Override
  public void emitTuples()
  {
    LOG.info("abc1: " + numTuplesTotal + maxTuplesTotal + numTuples + maxTuples);
    if (numTuplesTotal < maxTuplesTotal && numTuples < maxTuples) {
      LOG.info("abc2");
      ++numTuplesTotal;
      ++numTuples;
      out.emit(String.valueOf(numTuplesTotal));
    } else {

      try {
        // avoid repeated calls to this function
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.info("Sleep interrupted");
      }
    }
  }

  public int getMaxTuples() { return maxTuples; }
  public void setMaxTuples(int v) { maxTuples = v; }

  public int getMaxTuplesTotal() { return maxTuplesTotal; }
  public void setMaxTuplesTotal(int v) { maxTuplesTotal = v; }
}
