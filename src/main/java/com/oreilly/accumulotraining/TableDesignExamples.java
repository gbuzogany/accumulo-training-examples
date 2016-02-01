package com.oreilly.accumulotraining;

import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map.Entry;
import java.util.Random;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.hadoop.io.Text;

import static com.google.common.collect.Lists.newArrayList;

public class TableDesignExamples {

  // Time-Ordered Data
  
  private static final DateLexicoder dateLexicoder = new DateLexicoder();
  private static final Random random = new Random();
  
  public static class TimeValue implements Comparable<TimeValue> {
    public Date time;
    public Value value;
    
    public TimeValue(final Date time, final Value value) {
      this.time = time;
      this.value = value;
    }

    @Override
    public int compareTo(TimeValue o) {
      return this.time.compareTo(o.time);
    }
  }
  
  private static byte[] rowToBucketRow(final byte [] row, int bucketID) {
    
    byte[] bucketBytes = String.format("%05d", bucketID).getBytes();
    
    byte[] combined = new byte[row.length + bucketBytes.length];
    System.arraycopy(row, 0, combined, 0, row.length);
    System.arraycopy(bucketBytes, 0, combined, row.length, bucketBytes.length);

    return combined;
  }
  
  /**
   * 
   * Assigns one of a fixed number of bucket prefixes 
   * to Mutations uniformly and writes to table
   * 
   * @param writer
   * @param buckets
   * @param timeValue
   * @throws MutationsRejectedException 
   */
  public static void writeToTimeOrderedTable(
          final BatchWriter writer, 
          int buckets, 
          final TimeValue timeValue) 
          throws MutationsRejectedException {

    int bucket = random.nextInt(buckets);

    byte[] rowBytes = dateLexicoder.encode(timeValue.time);
    
    Mutation m = new Mutation(rowToBucketRow(rowBytes, bucket));
    m.put("", "", timeValue.value);
    writer.addMutation(m);
  }

  /**
   * Reads time-ordered data from each bucket using a BatchScanner
   * 
   * @param batchScanner
   * @param buckets
   * @param start
   * @param stop
   * @return 
   */
  public static Iterable<TimeValue> readTimeOrderedTable(
          final BatchScanner batchScanner,
          int buckets,
          final Date start,
          final Date stop) {

    final byte[] startBytes = dateLexicoder.encode(start);
    final byte[] stopBytes = dateLexicoder.encode(stop);
    
    ContiguousSet<Integer> bucketSet
            = ContiguousSet.create(
                    com.google.common.collect.Range.closed(0, buckets),
                    DiscreteDomain.integers());

    batchScanner.setRanges(
            newArrayList(
                    Iterables.transform(bucketSet,
                            new Function<Integer, Range>() {

                              @Override
                              public Range apply(Integer bucket) {
                                return new Range(
                                        new Text(rowToBucketRow(startBytes, bucket)),
                                        new Text(rowToBucketRow(stopBytes, bucket)));
                              }
                            })));

    // merge into a single time-ordered view
    ArrayList<TimeValue> results = 
            newArrayList(Iterables.transform(batchScanner, KEY_VALUE_TO_TIME_VALUE));
    
    Collections.sort(results);
    
    return results;
  }
  
  private static byte[] bucketRowToRow(final byte[] bucketRow) {
    
    byte[] row = new byte[bucketRow.length - 5];
    
    System.arraycopy(bucketRow, 0, row, 5, row.length);
    return row;
  }
  
  private static class KeyValueToTimeValue implements Function<Entry<Key, Value>, TimeValue> {

    @Override
    public TimeValue apply(Entry<Key, Value> e) {
      return new TimeValue(
              dateLexicoder.decode(bucketRowToRow(e.getKey().getRow().getBytes())), 
              e.getValue());
    }
  }
  
  private static final KeyValueToTimeValue KEY_VALUE_TO_TIME_VALUE = new KeyValueToTimeValue();

  
  
  // Graphs
  
  public static class Edge {

    public final String nodeA;
    public final String edgeType;
    public final String nodeB;
    public final long weight;

    public Edge(String nodeA, String edgeType, String nodeB, long weight) {
      this.nodeA = nodeA;
      this.edgeType = edgeType;
      this.nodeB = nodeB;
      this.weight = weight;
    }
  }
  
  /**
   * 
   * Converts an edge to a Mutation and writes to table
   * 
   * @param writer
   * @param edge
   * @param directed
   * @throws MutationsRejectedException 
   */
  public static void writeToGraphTable(
          final BatchWriter writer,
          final Edge edge,
          boolean directed) throws MutationsRejectedException {

    Mutation forwardEdge = new Mutation(edge.nodeA);
    forwardEdge.put(edge.edgeType, edge.nodeB, new Value(Long.toString(edge.weight).getBytes()));

    writer.addMutation(forwardEdge);

    // optionally write reverse edge for an undirected graph
    if (!directed) {
      Mutation reverseEdge = new Mutation(edge.nodeB);
      reverseEdge.put(edge.edgeType, edge.nodeA, new Value(Long.toString(edge.weight).getBytes()));

      writer.addMutation(reverseEdge);
    }
  }
  
  /**
   * 
   * Configures a table for receiving weighted graph edges
   * 
   * @param connector
   * @param table
   * @throws AccumuloSecurityException
   * @throws AccumuloException
   * @throws TableNotFoundException 
   */
  public static void setupSummingIterator(final Connector connector, String table)
          throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

    // setup combiner to sum weights of unique edges
    IteratorSetting iterSet = new IteratorSetting(10, table, LongCombiner.class);
    LongCombiner.setEncodingType(iterSet, Type.STRING);

    connector.tableOperations().attachIterator(table, iterSet);

    connector.tableOperations().removeIterator(table, "vers", EnumSet.allOf(IteratorScope.class));
  }


  /**
   * 
   * Fetch neighbors of a node using a simple scan
   * 
   * @param scanner
   * @param nodeA
   * @return 
   */
  public static Iterable<Edge> neighborsOfNode(final Scanner scanner, final String nodeA) {

    // all neighbors of nodeA are stored in one row
    scanner.setRange(Range.exact(nodeA));

    // convert key-value pairs to Edge objects
    return Iterables.transform(scanner, KEY_VALUE_TO_EDGE);
  }

  private static class KeyValueToEdge implements Function<Entry<Key, Value>, Edge> {

    @Override
    public Edge apply(Entry<Key, Value> f) {
      return new Edge(
              f.getKey().getRow().toString(),
              f.getKey().getColumnFamily().toString(),
              f.getKey().getColumnQualifier().toString(),
              Integer.parseInt(new String(f.getValue().get())));
    }
  }
  
  private static final KeyValueToEdge KEY_VALUE_TO_EDGE = new KeyValueToEdge();
  
  /**
   * Fetch neighbors of a set of neighbors using a BatchScanner
   * 
   * @param neighbors
   * @param batchScanner
   * @return 
   */
  public static Iterable<Edge> neighborsOfNeighbors(
          final Iterable<Edge> neighbors,
          final BatchScanner batchScanner) {

    // create one range per neighbor
    batchScanner.setRanges(
            newArrayList(
                    Iterables.transform(neighbors, new Function<Edge, Range>() {

                      @Override
                      public Range apply(Edge f) {
                        return Range.exact(f.nodeB);
                      }
                    })));

    return Iterables.transform(batchScanner, KEY_VALUE_TO_EDGE);
  }



}
