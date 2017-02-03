package com.example;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by oliver on 1/31/17.
 */
public class PassthroughFailOperator extends BaseOperator
{

  private static final Logger LOG = LoggerFactory.getLogger(PassthroughFailOperator.class);
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

  private boolean hasBeenKilledString;
  int tuplesUntilKill = 5;

  @NotNull
  private String directoryPath;
  private String filePath;

  transient FileSystem hdfs;
  transient Path filePathObj;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    String appId = context.getValue(Context.DAGContext.APPLICATION_ID);
    filePath = directoryPath + appId;

    LOG.info("FilePath: " + filePath);
    try {
      hdfs = FileSystem.newInstance(new Path(filePath).toUri(), new Configuration());
    } catch (IOException e) {
      e.printStackTrace();
    }

    filePathObj = new Path(filePath);
    try {
      if (hdfs.exists(filePathObj)) {
        hasBeenKilledString = true;
        LOG.info("file already exists -> Operator has been killed before");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public final transient DefaultInputPort<String>
    input = new DefaultInputPort<String>()
  {

    @Override
    public void process(String line)
    {
      LOG.info("LINE " + line);
      if (!hasBeenKilledString && tuplesUntilKill <= 0) {
        try {
          hdfs.createNewFile(filePathObj);
          LOG.info("created file for hasBeenKilled state");
        } catch (IOException e) {
          e.printStackTrace();
        }
        //kill operator
        LOG.info("OPERATOR KILLED THROUGH EXCEPTION");
        RuntimeException e = new RuntimeException();
        throw e;
      }
      output.emit(line);
      tuplesUntilKill--;
    }
  };

  public String getDirectoryPath()
  {
    return directoryPath;
  }

  public void setDirectoryPath(String directoryPath)
  {
    this.directoryPath = directoryPath;
  }

}

