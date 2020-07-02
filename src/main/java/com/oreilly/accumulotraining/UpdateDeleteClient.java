package com.oreilly.accumulotraining;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.accumulo.core.client.*;
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

			AccumuloClient conn = Accumulo.newClient()
					.to(instanceName, zookeepers)
					.as(username, password).build();
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
		catch (MutationsRejectedException | TableNotFoundException ex) {
			Logger.getLogger(IngestClient.class.getName()).log(Level.SEVERE, null, ex);
		}
	}		
}
