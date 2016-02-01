package com.oreilly.accumulotraining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ErrorHandlingExample {
  
  private static final Logger logger = LoggerFactory.getLogger(ErrorHandlingExample.class);
  private static final String DATA_TABLE = "data";
  private static final int BATCH_SIZE = 1000;
  
  private final String instanceName;
  private final String zookeepers;
  private final String username;
  private final String password;
  
  private ZooKeeperInstance inst;
  private Connector conn;
  private DataGenerator incomingData;
  private ArrayList<Mutation> batch;
  private BatchWriter dataWriter = null;
  
  public ErrorHandlingExample(
          String instanceName,
          String zookeepers,
          String username,
          String password) {
  
    this.instanceName = instanceName;
    this.zookeepers = zookeepers;
    this.username = username;
    this.password = password;
  }
  
  public void run() throws TableNotFoundException, AccumuloException, AccumuloSecurityException, TableExistsException, InterruptedException, IOException {
    
    inst = new ZooKeeperInstance(instanceName, zookeepers);
    conn = inst.getConnector(username, new PasswordToken(password));
    
    if(!conn.tableOperations().exists(DATA_TABLE)) {
      conn.tableOperations().create(DATA_TABLE);
    }

    BatchWriterConfig dataWriterConfig = new BatchWriterConfig();
    dataWriterConfig.setMaxLatency(10, TimeUnit.SECONDS);
    dataWriterConfig.setMaxMemory(10240);
    // writes survive single server failure
    dataWriterConfig.setDurability(Durability.FLUSH);
    dataWriterConfig.setMaxWriteThreads(10);

    // this causes our batch writer to get an exception rather than waiting forever
    //dataWriterConfig.setTimeout(5, TimeUnit.SECONDS);

    dataWriter = null;
    incomingData = new DataGenerator(10000);

    while (incomingData.hasNext()) {

      Mutation m = new Mutation(incomingData.next());

      m.put("colFam",
              "colQual",
              new Value("value".getBytes()));
      batch.add(m);

      if (batch.size() >= BATCH_SIZE) {

        if(dataWriter == null) {
          dataWriter = conn.createBatchWriter(DATA_TABLE, dataWriterConfig);
        }
        
        try {

          dataWriter.addMutations(batch);

          dataWriter.flush();
          batch.clear();

          return;

        } catch (MutationsRejectedException ex) {

          if (!handleExceptions(ex)) {
            // print out info from batch that failed
            dumpBatchToScreen(batch);
            
            throw new IOException("There are permanent errors .. exiting");
          }
          else {
            // can optionally retry the batch
          }

          // this BatchWriter won't be able to reconnect after errors
          try {
            dataWriter.close();
          } catch (Exception ex1) {
            logger.warn("Additional error closing batch writer: " + ex1.getMessage());
          }
          
          dataWriter = null;
        }
      }
    }    
  }
  
  
  
  /**
   * This method differentiates between various types of exception we may see
   * and returns true if we could retry, false if we should exit.
   * 
   * @param ex
   * @return 
   */
  private boolean handleExceptions(MutationsRejectedException ex) {

    // ---- permanent failures ----
    
    Map<TabletId, Set<SecurityErrorCode>> securityErrors = ex.getSecurityErrorCodes();
    for (Map.Entry<TabletId, Set<SecurityErrorCode>> entry : securityErrors.entrySet()) {
      for (SecurityErrorCode err : entry.getValue()) {
        logger.error("permanent error: " + err.toString());
      }
    }

    List<ConstraintViolationSummary> constraintViolations = ex.getConstraintViolationSummaries();

    if (!securityErrors.isEmpty() || !constraintViolations.isEmpty()) {
      logger.error("Have permanent errors. Exiting ...");
      return false;
    }

    // ---- transient failures ----
    
    Collection<String> errorServers = ex.getErrorServers();
    for (String errorServer : errorServers) {
      logger.warn("Problem with server: " + errorServer);
    }

    int numUnknownExceptions = ex.getUnknownExceptions();
    if (numUnknownExceptions > 0) {
      logger.warn(numUnknownExceptions + " unknown exceptions.");
    }

    return true;
  }

  private void dumpBatchToScreen(ArrayList<Mutation> batch) {
    for(Mutation m : batch) {
      System.out.println(new String(m.getRow()));
    }
  }

  /**
   * Just generates some strings we can write to a table
   * 
   */
  private static class DataGenerator implements Iterator<String> {

    private final int totalMutations;
    private int mutationsWritten = 0;

    public DataGenerator(int totalMutations) {
      this.totalMutations = totalMutations;
    }

    @Override
    public boolean hasNext() {
      return mutationsWritten < totalMutations;
    }

    @Override
    public String next() {
      return Integer.toString(mutationsWritten++);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
