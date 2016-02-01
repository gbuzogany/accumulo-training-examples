package com.oreilly.accumulotraining;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;

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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class IngestClient {
	
	private static final Logger logger = Logger.getLogger(IngestClient.class.getName());
	
	private static final int COUNTRY = 0;
	private static final int COMMODITY = 1;
	private static final int YEAR = 2;
	private static final int UNIT = 3;
	private static final int QUANTITY = 4;
	private static final int FOOTNOTES = 5;
	
	public static void run(
			String instanceName, 
			String zookeepers, 
			String username, 
			String password, 
			String table, 
			String filename) {
		
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
			
			int written = 0;
			File csvData = new File(filename);
			
			System.out.println("parsing file");
			CSVParser parser = CSVParser.parse(csvData, Charset.defaultCharset(), CSVFormat.EXCEL);
			
			System.out.println("writing data from file " + filename + " ...");
			
			Iterator<CSVRecord> records = parser.getRecords().iterator();
			records.next(); // skip header
			
			while(records.hasNext()) {
				CSVRecord csvRecord = records.next();
				
				if(csvRecord.size() < 6) {
					continue;
				}
				
				String energyType = parseEnergyType(csvRecord.get(COMMODITY));
				Mutation m = new Mutation(csvRecord.get(COUNTRY));
				
				m.put(
						energyType, 
						csvRecord.get(YEAR), 
						new Value(csvRecord.get(QUANTITY).getBytes()));
				
				writer.addMutation(m);
				written++;
			}
			
			writer.close();
			parser.close();
			
			System.out.println("wrote " + written + " records");
		
		} catch (MutationsRejectedException ex) {
			
			// see Error Handling Example
			
		} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException | IOException ex) {
			logger.log(Level.SEVERE, ex.getLocalizedMessage(), ex);
		}
	}	

	private static String parseEnergyType(String line) {
		String[] parts = line.split("\\s+");
		if(parts.length > 3)
			return parts[3];
		return line;
	}
}
