package com.oreilly.accumulotraining;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;

public class UpdateDeleteClient {

	public static void run(
			String instanceName, 
			String zookeepers, 
			String username, 
			String password, 
			String table, 
			String row,
			String columnFamily,
			String columnQualifier,
			String value,
			boolean delete) {
		
		try {
			
			Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
			
			Connector conn = inst.getConnector(username, new PasswordToken(password));
			
			BatchWriterConfig config = new BatchWriterConfig();
			config.setMaxLatency(1, TimeUnit.SECONDS);
			config.setMaxMemory(10240);
			config.setDurability(Durability.DEFAULT);
			config.setMaxWriteThreads(10);
			
			BatchWriter writer = conn.createBatchWriter(table, config);
			
			Mutation m = new Mutation(row);
			if(delete) {
				m.putDelete(columnFamily, columnQualifier);
			}	
			else {
				m.put(columnFamily, columnQualifier, value);
			}
			
			writer.addMutation(m);
			writer.close();
		}
		catch (AccumuloException | AccumuloSecurityException | TableNotFoundException ex) {
			Logger.getLogger(IngestClient.class.getName()).log(Level.SEVERE, null, ex);
		}
	}		
}
