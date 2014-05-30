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
package org.apache.accumulo.cluster.micro;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class AccumuloMicroClusterTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    AccumuloMicroConfig microConfig = new AccumuloMicroConfig(folder.newFolder(), "");
    AccumuloMicroCluster micro = microConfig.build();

    micro.start();

    ZooKeeperInstance inst = new ZooKeeperInstance(microConfig.getInstanceName(), micro.getZooKeepers());
    Connector conn = inst.getConnector("root", new PasswordToken(""));

    conn.tableOperations().create("foo");
    BatchWriter bw = conn.createBatchWriter("foo", new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("colf", "colq", "foobar");
    bw.addMutation(m);
    bw.close();

    Entry<Key,Value> entry = Iterables.getOnlyElement(conn.createScanner("foo", Authorizations.EMPTY));
    Assert.assertEquals(0, new Key("row", "colf", "colq").compareTo(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
    Assert.assertEquals(new Value("foobar".getBytes()), entry.getValue());

    micro.stop();
  }

}
