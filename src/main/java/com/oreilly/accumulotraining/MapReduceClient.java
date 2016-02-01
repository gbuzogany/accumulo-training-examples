package com.oreilly.accumulotraining;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceClient implements Tool {

  private Configuration conf = null;

  private final String inputTable;
  private final String instanceName;
  private final String zookeepers;
  private final String username;
  private final String password;
  private final String outputTable;

  private MapReduceClient(
          String instanceName,
          String zookeepers,
          String username,
          String password,
          String inputTable,
          String outputTable) {

    this.instanceName = instanceName;
    this.zookeepers = zookeepers;
    this.username = username;
    this.password = password;
    this.inputTable = inputTable;
    this.outputTable = outputTable;
  }

  @Override
  public int run(String[] args) throws Exception {

    PasswordToken passwordToken = new PasswordToken(password);
    ClientConfiguration config = new ClientConfiguration();
    config.setProperty(ClientProperty.INSTANCE_NAME, instanceName);
    config.setProperty(ClientProperty.INSTANCE_ZK_HOST, zookeepers);

    Job job = Job.getInstance(getConf());
    
    AccumuloInputFormat.setZooKeeperInstance(job, config);
    AccumuloInputFormat.setConnectorInfo(job, username, passwordToken);
    AccumuloInputFormat.setInputTableName(job, inputTable);
    
    job.setInputFormatClass(AccumuloInputFormat.class);
    
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setReducerClass(MyReducer.class);

    AccumuloOutputFormat.setZooKeeperInstance(job, config);
    AccumuloOutputFormat.setConnectorInfo(job, username, passwordToken);
    AccumuloOutputFormat.setDefaultTableName(job, outputTable);
    AccumuloOutputFormat.setCreateTables(job, true);

    job.setOutputFormatClass(AccumuloOutputFormat.class);
    
    job.waitForCompletion(true);
    
    return 0;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    if (conf == null) {
      conf = new Configuration();
    }

    return conf;
  }

  public static void execute(
          String instanceName,
          String zookeepers,
          String username,
          String password,
          String inputTable,
          String outputTable) {

    try {
      
      ToolRunner.run(new MapReduceClient(
              instanceName,
              zookeepers,
              username,
              password,
              inputTable,
              outputTable), null);
      
    } catch (Exception ex) {
      Logger.getLogger(MapReduceClient.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private static class MyMapper extends Mapper<Key, Value, Text,DoubleWritable> {

    public MyMapper() {
    }

    @Override
    public void map(Key key, Value value, Context context) throws IOException, InterruptedException {
      // write out energy type and amount of energy
      context.write(key.getColumnFamily(), new DoubleWritable(Double.parseDouble(new String(value.get()))));
    }
  }

  private static class MyReducer extends Reducer<Text, DoubleWritable, Text, Mutation> {

    public MyReducer() {
    }

    // sum over total millions of kilowatt hours per energy type
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

      Double sum = 0.0;

      for (DoubleWritable value : values) {
        sum += value.get();
      }

      Mutation m = new Mutation(key);

      m.put("total", "", new Value(Double.toString(sum).getBytes()));

      context.write(null, m);
    }
  }
}
