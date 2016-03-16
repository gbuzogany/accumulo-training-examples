package com.oreilly.accumulotraining;

import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

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
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;


public class ReplicationDataGenerator {
	
	private static final Logger logger = Logger.getLogger(ReplicationDataGenerator.class.getName());
	
	
	public static void run(
			String instanceName, 
			String zookeepers, 
			String username, 
			String password, 
			String table) {
		
		try {
			
			System.out.println("connecting to accumulo ...");
			Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
			System.out.println("got instance");
			Connector conn = inst.getConnector(username, new PasswordToken(password));
			System.out.println("got connector");
			
			if(!conn.tableOperations().exists(table)) {
				System.out.println("creating table " + table);
				conn.tableOperations().create(table);
			}
			
			BatchWriterConfig config = new BatchWriterConfig();
			config.setMaxLatency(1, TimeUnit.SECONDS);
			config.setMaxMemory(10240);
			config.setDurability(Durability.DEFAULT);
			config.setMaxWriteThreads(10);
			
			BatchWriter writer = conn.createBatchWriter(table, config);
			
      Random random = new Random();
      StringBuilder dummyStringBuilder = new StringBuilder();
      for(int i=0; i < 1024; i++) {
        
        dummyStringBuilder.append((char)(random.nextInt(26) + 'a'));
      }
      
      final Value dummyValue = new Value(dummyStringBuilder.toString().getBytes());
			
			
			Iterator<Mutation> records = new Iterator<Mutation>() {
        
        private int written = 0;
        
        @Override
        public boolean hasNext() {
          return written < 50000; // write 50,000 1k records
        }

        @Override
        public Mutation next() {
          Mutation m = new Mutation(Integer.toString(written++));
          m.put("", "", dummyValue);
          return m;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
      };
      
			System.out.println("Writing 50,000 1k records ...");
			while(records.hasNext()) {
				
				writer.addMutation(records.next());
			}
			
			writer.close();
			System.out.println("done.");
      
		} catch (MutationsRejectedException ex) {
			
			// see Error Handling Example
			
		} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException ex) {
			logger.log(Level.SEVERE, ex.getLocalizedMessage(), ex);
		}
	}	
}
