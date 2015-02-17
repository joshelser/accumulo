/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.ServerConfigurationUtil;
import org.apache.accumulo.core.client.impl.ThriftTransportKey;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.SslConnectionParams;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint;
import org.apache.accumulo.examples.simple.constraints.NumericValueConstraint;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.io.Text;
import org.apache.thrift.transport.TTransport;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstraintIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(ConstraintIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  }

  @Test
  public void run() throws Exception {
    long timeout = AccumuloConfiguration.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT.getDefaultValue());
    String[] tableNames = getUniqueNames(3);
    Credentials credentials = new Credentials(getPrincipal(), getToken());
    Connector c = getConnector();
    Instance instance = c.getInstance();
    TCredentials tcreds = credentials.toThrift(instance);
    for (String table : tableNames) {
      c.tableOperations().create(table);
      c.tableOperations().addConstraint(table, NumericValueConstraint.class.getName());
      c.tableOperations().addConstraint(table, AlphaNumKeyConstraint.class.getName());
    }

    ArrayList<ThriftTransportKey> servers = new ArrayList<ThriftTransportKey>();
    ZooCache zc = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    for (String tserver : zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTSERVERS)) {
      String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + tserver;
      byte[] data = ZooUtil.getLockData(zc, path);
      if (data != null && !new String(data, UTF_8).equals("master"))
        servers
            .add(new ThriftTransportKey(new ServerServices(new String(data)).getAddressString(Service.TSERV_CLIENT), timeout, SslConnectionParams.forClient(ServerConfigurationUtil
                .getConfiguration(instance))));
    }

    final int expectedConstraints = 3;
    ThriftTransportPool pool = ThriftTransportPool.getInstance();
    while (true) {
      Map<String,Integer> constraintCountsByTable = new HashMap<String,Integer>();

      // For each server
      for (ThriftTransportKey ttk : servers) {
        TTransport transport = null;
        try {
          // Open a transport
          transport = pool.getTransport(ttk.getLocation() + ":" + ttk.getPort(), ttk.getTimeout(), ttk.getSslParams());
          TabletClientService.Client client = ThriftUtil.createClient(new TabletClientService.Client.Factory(), transport);

          for (String tableName : tableNames) {
              // convert the name to an id
            String tableId = c.tableOperations().tableIdMap().get(tableName);
            assertNotNull("Found no mapping for table", tableName);
            List<String> constraints = client.getActiveConstraints(Tracer.traceInfo(), tcreds, tableId);

            log.info("Verified {} constraints for {} on {}", constraints.size(), tableId, ttk);
            if (0 != constraints.size()) {
              constraintCountsByTable.put(tableName, constraints.size());
            }
          }
        } finally {
          if (null != transport) {
            pool.returnTransport(transport);
          }
        }
      }

      boolean failed = false;
      for (Entry<String,Integer> entry : constraintCountsByTable.entrySet()) {
        if (expectedConstraints != entry.getValue()) {
          log.warn("{} only had {} constraints, retrying", entry.getKey(), entry.getValue());
          failed = true;
        }
      }

      if (constraintCountsByTable.size() != tableNames.length) {
        log.warn("Missing constraints for tables");
      } else if (!failed) {
        log.info("Found all constraints");
        break;
      }

      Thread.sleep(1000);
    }

    // Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
    // logger.setLevel(Level.TRACE);

    test1(tableNames[0]);

    // logger.setLevel(Level.TRACE);

    test2(tableNames[1], false);
    test2(tableNames[2], true);
  }

  private void test1(String tableName) throws Exception {
    BatchWriter bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());

    Mutation mut1 = new Mutation(new Text("r1"));
    mut1.put(new Text("cf1"), new Text("cq1"), new Value("123".getBytes(UTF_8)));

    bw.addMutation(mut1);

    // should not throw any exceptions
    bw.close();

    bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());

    // create a mutation with a non numeric value
    Mutation mut2 = new Mutation(new Text("r1"));
    mut2.put(new Text("cf1"), new Text("cq1"), new Value("123a".getBytes(UTF_8)));

    bw.addMutation(mut2);

    boolean sawMRE = false;

    try {
      bw.close();
      // should not get here
      throw new Exception("Test failed, constraint did not catch bad mutation");
    } catch (MutationsRejectedException mre) {
      sawMRE = true;

      // verify constraint violation summary
      List<ConstraintViolationSummary> cvsl = mre.getConstraintViolationSummaries();

      if (cvsl.size() != 1) {
        throw new Exception("Unexpected constraints");
      }

      for (ConstraintViolationSummary cvs : cvsl) {
        if (!cvs.constrainClass.equals(NumericValueConstraint.class.getName())) {
          throw new Exception("Unexpected constraint class " + cvs.constrainClass);
        }

        if (cvs.numberOfViolatingMutations != 1) {
          throw new Exception("Unexpected # violating mutations " + cvs.numberOfViolatingMutations);
        }
      }
    }

    if (!sawMRE) {
      throw new Exception("Did not see MutationsRejectedException");
    }

    // verify mutation did not go through
    Scanner scanner = getConnector().createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(new Range(new Text("r1")));

    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    Entry<Key,Value> entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("123".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }

    // remove the numeric value constraint
    getConnector().tableOperations().removeConstraint(tableName, 2);
    UtilWaitThread.sleep(1000);

    // now should be able to add a non numeric value
    bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());
    bw.addMutation(mut2);
    bw.close();

    // verify mutation went through
    iter = scanner.iterator();
    entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("123a".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }

    // add a constraint that references a non-existant class
    getConnector().tableOperations().setProperty(tableName, Property.TABLE_CONSTRAINT_PREFIX + "1", "com.foobar.nonExistantClass");
    UtilWaitThread.sleep(1000);

    // add a mutation
    bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());

    Mutation mut3 = new Mutation(new Text("r1"));
    mut3.put(new Text("cf1"), new Text("cq1"), new Value("foo".getBytes(UTF_8)));

    bw.addMutation(mut3);

    sawMRE = false;

    try {
      bw.close();
      // should not get here
      throw new Exception("Test failed, mutation went through when table had bad constraints");
    } catch (MutationsRejectedException mre) {
      sawMRE = true;
    }

    if (!sawMRE) {
      throw new Exception("Did not see MutationsRejectedException");
    }

    // verify the mutation did not go through
    iter = scanner.iterator();
    entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("123a".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }

    // remove the bad constraint
    getConnector().tableOperations().removeConstraint(tableName, 1);
    UtilWaitThread.sleep(1000);

    // try the mutation again
    bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());
    bw.addMutation(mut3);
    bw.close();

    // verify it went through
    iter = scanner.iterator();
    entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("foo".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }
  }

  private Mutation newMut(String row, String cf, String cq, String val) {
    Mutation mut1 = new Mutation(new Text(row));
    mut1.put(new Text(cf), new Text(cq), new Value(val.getBytes(UTF_8)));
    return mut1;
  }

  private void test2(String table, boolean doFlush) throws Exception {
    // test sending multiple mutations with multiple constrain violations... all of the non violating mutations
    // should go through
    int numericErrors = 2;

    BatchWriter bw = getConnector().createBatchWriter(table, new BatchWriterConfig());
    bw.addMutation(newMut("r1", "cf1", "cq1", "123"));
    bw.addMutation(newMut("r1", "cf1", "cq2", "I'm a bad value"));
    if (doFlush) {
      try {
        bw.flush();
        throw new Exception("Didn't find a bad mutation");
      } catch (MutationsRejectedException mre) {
        // ignored
        try {
          bw.close();
        } catch (MutationsRejectedException ex) {
          // ignored
        }
        bw = getConnector().createBatchWriter(table, new BatchWriterConfig());
        numericErrors = 1;
      }
    }
    bw.addMutation(newMut("r1", "cf1", "cq3", "I'm a naughty value"));
    bw.addMutation(newMut("@bad row@", "cf1", "cq2", "456"));
    bw.addMutation(newMut("r1", "cf1", "cq4", "789"));

    boolean sawMRE = false;

    try {
      bw.close();
      // should not get here
      throw new Exception("Test failed, constraint did not catch bad mutation");
    } catch (MutationsRejectedException mre) {
      System.out.println(mre);

      sawMRE = true;

      // verify constraint violation summary
      List<ConstraintViolationSummary> cvsl = mre.getConstraintViolationSummaries();

      if (cvsl.size() != 2) {
        Assert.fail("Unexpected constraints:  " + cvsl);
      }

      HashMap<String,Integer> expected = new HashMap<String,Integer>();

      expected.put("org.apache.accumulo.examples.simple.constraints.NumericValueConstraint", numericErrors);
      expected.put("org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint", 1);

      for (ConstraintViolationSummary cvs : cvsl) {
        if (expected.get(cvs.constrainClass) != cvs.numberOfViolatingMutations) {
          throw new Exception("Unexpected " + cvs.constrainClass + " " + cvs.numberOfViolatingMutations);
        }
      }
    }

    if (!sawMRE) {
      throw new Exception("Did not see MutationsRejectedException");
    }

    Scanner scanner = getConnector().createScanner(table, Authorizations.EMPTY);

    Iterator<Entry<Key,Value>> iter = scanner.iterator();

    Entry<Key,Value> entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("123".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq4")) || !entry.getValue().equals(new Value("789".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }

  }

}
