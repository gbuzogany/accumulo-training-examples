package com.oreilly.accumulotraining;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.io.Text;


public class OptimizationExamples {

  public static void putColumnFamiliesIntoLocalityGroup(
          final Connector connector,
          final String table,
          final Set<Text> columnFamilies,
          final String group) 
          throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    
    
    Map<String, Set<Text>> localityGroups = connector.tableOperations().getLocalityGroups(table);
    
    // add column families to a group, preserving any existing column families in the group
    Set<Text> combinedColumnFamilies = columnFamilies;
    
    if(localityGroups.containsKey(group)) {
      combinedColumnFamilies = localityGroups.get(group);
      combinedColumnFamilies.addAll(columnFamilies);
    }
    
    localityGroups.put(group, combinedColumnFamilies);
    connector.tableOperations().setLocalityGroups(group, localityGroups);
    
  }
  
  public static void enableBloomFilter(
          final Connector connector,
          String table) 
          throws AccumuloException, AccumuloSecurityException {
    
    connector.tableOperations().setProperty(table, "table.bloom.enabled", "true");
  }
  
  public static void enableBlockCaching(
          final Connector connector,
          String table) 
          throws AccumuloException, AccumuloSecurityException {
    
    connector.tableOperations().setProperty(table, "table.cache.block.enable", "true");
  }
  
  public static void splitTable(
          final Connector connector,
          final String table,
          final SortedSet<Text> splitPoints) 
          throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    
    connector.tableOperations().addSplits(table, splitPoints);
  }
  
  
}
