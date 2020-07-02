package com.oreilly.accumulotraining;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class ScanClient {
	
	private static final Logger logger = Logger.getLogger(ScanClient.class.getName());
	
	public static void run(
			String instanceName,
			String zookeepers,
			String username,
			String password,
			String table,
			String row,
			String columnFamily,
			String columnQualifier) {
		
		try {
			AccumuloClient conn = Accumulo.newClient()
					.to(instanceName, zookeepers)
					.as(username, password).build();
			Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
			
			if(row != null) {
				scanner.setRange(Range.exact(row));
			}
			
			if(columnFamily != null) {
				if(columnQualifier != null) {
					scanner.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
				}
				else {
					scanner.fetchColumnFamily(new Text(columnFamily));
				}
			}
			
			for(Map.Entry<Key, Value> e : scanner) {
				System.out.println(
						e.getKey().getRow().toString() + " " +
						e.getKey().getColumnFamily().toString() + " " +
						e.getKey().getColumnQualifier().toString() + "\t" +
						new String(e.getValue().get()));
			}
			
		
		} catch (TableNotFoundException ex) {
			logger.log(Level.SEVERE, ex.getLocalizedMessage(), ex);
		}
	}

}
