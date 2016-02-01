package com.oreilly.accumulotraining;

import com.google.common.base.Function;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class IndexScanClient {

	private static final Logger logger = Logger.getLogger(IndexScanClient.class.getName());

	public static void run(
			String instanceName,
			String zookeepers,
			String username,
			String password,
			String table,
			String startValue,
			String endValue) {

		try {
			Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
			Connector conn = inst.getConnector(username, new PasswordToken(password));

			// scan index table first
			Scanner scanner = conn.createScanner(table + "_index", Authorizations.EMPTY);
			
			DoubleLexicoder doubleLexicoder = new DoubleLexicoder();
			
			Range range = new Range(
					new Text(doubleLexicoder.encode(Double.parseDouble(startValue))), 
					new Text(doubleLexicoder.encode(Double.parseDouble(endValue))));
			
			
			scanner.setRange(range);
			
			// get identifying keys for main table key-value pairs
			ArrayList<Range> results = newArrayList(
					transform(
							transform(
									scanner,
									MAIN_KEY_FOR_INDEX_ENTRY),
							EXACT_RANGE_FOR_KEY));
			
			if(results.isEmpty()) {
				System.out.println("no results");
				return;
			}
			
			System.out.println("Got " + results.size() + " results\nFirst 20 results:\n");
			
			BatchScanner batchScanner = conn.createBatchScanner(table, Authorizations.EMPTY, 10);
			
			batchScanner.setRanges(results);
			
			
			for(Entry<Key, Value> result : limit(batchScanner, 20)) {
				Key k = result.getKey();
				System.out.println(
						k.getRow().toString() + " " +
						k.getColumnFamily().toString() + " " +
						k.getColumnQualifier().toString() + "\t" +
						new String(result.getValue().get()));
			}
			
		} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException ex) {
			Logger.getLogger(IndexScanClient.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public static class MainKeyForIndexEntry implements Function<Entry<Key, Value>, Key> {

		@Override
		public Key apply(Entry<Key, Value> e) {
			Key k = e.getKey();
			String[] parts = k.getColumnQualifier().toString().split("\t");
			
			return new Key(new Text(parts[0]), k.getColumnFamily(), new Text(parts[1]));
		}		
	}
	
	private static final MainKeyForIndexEntry MAIN_KEY_FOR_INDEX_ENTRY = new MainKeyForIndexEntry();
	
	public static class ExactRangeForKey implements Function<Key, Range> {

		@Override
		public Range apply(Key f) {
			return Range.exact(f.getRow(), f.getColumnFamily(), f.getColumnQualifier());
		}
	}
	
	private static final ExactRangeForKey EXACT_RANGE_FOR_KEY = new ExactRangeForKey();
}