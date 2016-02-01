package com.oreilly.accumulotraining;

import java.util.Map;
import java.util.SortedSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class TableCode {

	public static void main(String[] args) {

		Map<String, String> properties = null;
		
		Instance instance = new ZooKeeperInstance("instance name", "zookeeper servers");

		try {
			Connector connector = instance.getConnector("username", new PasswordToken("password"));

			TableOperations ops = connector.tableOperations();

			SortedSet<String> tables = ops.list();
			for(String table : tables) {
				System.out.println(table);
			}			

			if (!ops.exists("table")) {
				ops.create("table");
				
				NewTableConfiguration tableConfiguration = new NewTableConfiguration();
				
				tableConfiguration.setProperties(properties);
				tableConfiguration.setTimeType(TimeType.MILLIS);
				
				ops.create("table", tableConfiguration);
			}

			ops.delete("table");
			
			
			Text startKey = null;
			Text endKey = null;
			
			ops.deleteRows("table", startKey, endKey);
			
			
			boolean wait = false;
			
			ops.flush("table", startKey, endKey, wait);

			
			boolean flush = true;
			
			ops.compact("table", startKey, endKey, flush, wait);


			
		} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException ex) {
			Logger.getLogger(TableCode.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
