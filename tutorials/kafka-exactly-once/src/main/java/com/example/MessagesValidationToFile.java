package com.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.io.fs.AbstractSingleFileOutputOperator;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Created by oliver on 2/14/17.
 */
public class MessagesValidationToFile extends AbstractSingleFileOutputOperator<KeyValPair<String, String>>
{
  private Map<String, List<String>> messagesMap = new HashMap<>();
  private String latestExactlyValue;
  private String latestAtLeastValue;

  @NotNull
  private String maxTuplesTotal;
  List<String> exactlyList;
  List<String> atLeastList;

  @Override
  public void teardown()
  {
    super.requestFinalize(outputFileName);
    super.teardown();

  }

  @Override
  protected byte[] getBytesForTuple(KeyValPair<String, String> pair)
  {
    final String topic = pair.getKey();
    final String value = new String(pair.getValue());

    if (topic.equals("exactly-once")) {
      latestExactlyValue = value;
      if (exactlyList == null) {
        exactlyList = new ArrayList<>();
      }
      exactlyList.add(value);
    }
    if (topic.equals("at-least-once")) {
      latestAtLeastValue = value;
      if (atLeastList == null) {
        atLeastList = new ArrayList<>();
      }
      atLeastList.add(value);
    }

    if (latestExactlyValue != null && latestAtLeastValue != null) {
      if (latestExactlyValue.equals(maxTuplesTotal) && latestAtLeastValue.equals(maxTuplesTotal)) {
        Set<String> exactlySet = new HashSet<>(exactlyList);
        Set<String> atLeastSet = new HashSet<>(atLeastList);

        int numDuplicatesExactly = exactlyList.size() - exactlySet.size();
        int numDuplicatesAtLeast = atLeastList.size() - atLeastSet.size();

        return ("Duplicates: exactly-once: " + numDuplicatesExactly + ", at-least-once: " + numDuplicatesAtLeast).getBytes();
      } else {
        return new byte[0];
      }
    } else {
      return new byte[0];

    }
  }

  public String getMaxTuplesTotal()
  {
    return maxTuplesTotal;
  }

  public void setMaxTuplesTotal(String maxTuplesTotal)
  {
    this.maxTuplesTotal = maxTuplesTotal;
  }
}
