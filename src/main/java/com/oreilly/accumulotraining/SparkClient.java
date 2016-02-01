package com.oreilly.accumulotraining;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkClient {

  public static void run(
          String instanceName,
          String zookeepers,
          String username,
          String password,
          String table) {

    try {
      SparkContext sc = new SparkContext(new SparkConf().setAppName("Spark Accumulo Client"));

      PasswordToken passwordToken = new PasswordToken(password);
      ClientConfiguration config = new ClientConfiguration();
      config.setProperty(ClientProperty.INSTANCE_NAME, instanceName);
      config.setProperty(ClientProperty.INSTANCE_ZK_HOST, zookeepers);

      Job job = Job.getInstance(new Configuration());

      AccumuloInputFormat.setInputTableName(job, table);
      AccumuloInputFormat.setConnectorInfo(job, username, passwordToken);
      AccumuloInputFormat.setZooKeeperInstance(job, config);

      JavaRDD<Tuple2<Key, Value>> rdd = sc.newAPIHadoopRDD(
              job.getConfiguration(),
              AccumuloInputFormat.class,
              Key.class,
              Value.class).toJavaRDD();

      JavaPairRDD<String, Double> output
              = rdd.mapToPair(new PairFunction<Tuple2<Key, Value>, String, Double>() {

                @Override
                public Tuple2<String, Double> call(Tuple2<Key, Value> t) throws Exception {
                  return new Tuple2(
                          t._1.getColumnFamily().toString(),
                          Double.parseDouble(new String(t._2.get())));
                }
              })
              .reduceByKey(new Function2<Double, Double, Double>() {

                @Override
                public Double call(Double t1, Double t2) throws Exception {
                  return t1 + t2;
                }
              });

      for (Tuple2<String, Double> entry : output.collect()) {
        System.out.println(entry._1 + ":\t" + entry._2);
      }

    } catch (AccumuloSecurityException | IOException ex) {
      Logger.getLogger(SparkClient.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
