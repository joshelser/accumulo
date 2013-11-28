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
package org.apache.accumulo.test.randomwalk;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.randomwalk.State;

import com.google.common.base.Preconditions;

public class AccumuloState {
  @SuppressWarnings("unused")
  private State state;
  private Properties props;
  
  private MultiTableBatchWriter mtbw = null;
  private Connector connector = null;
  private Instance instance = null;
  
  public AccumuloState(State state) {
    Preconditions.checkNotNull(state);
    
    // Hold onto the original State just in case we want/need it later
    this.state = state;
    
    this.props = state.getProperties();
  }
  
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    if (connector == null) {
      connector = getInstance().getConnector(getUserName(), getToken());
    }
    return connector;
  }
  
  public Credentials getCredentials() {
    return new Credentials(getUserName(), getToken());
  }
  
  public String getUserName() {
    return props.getProperty("USERNAME");
  }
  
  public AuthenticationToken getToken() {
    return new PasswordToken(props.getProperty("PASSWORD"));
  }
  
  public Instance getInstance() {
    if (instance == null) {
      String instance = props.getProperty("INSTANCE");
      String zookeepers = props.getProperty("ZOOKEEPERS");
      this.instance = new ZooKeeperInstance(new ClientConfiguration().withInstance(instance).withZkHosts(zookeepers));
    }
    return instance;
  }
  
  public MultiTableBatchWriter getMultiTableBatchWriter() {
    if (mtbw == null) {
      long maxMem = Long.parseLong(props.getProperty("MAX_MEM"));
      long maxLatency = Long.parseLong(props.getProperty("MAX_LATENCY"));
      int numThreads = Integer.parseInt(props.getProperty("NUM_THREADS"));
      mtbw = connector.createMultiTableBatchWriter(new BatchWriterConfig().setMaxMemory(maxMem).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS)
          .setMaxWriteThreads(numThreads));
    }
    return mtbw;
  }
  
  public String getMapReduceJars() {
    
    String acuHome = System.getenv("ACCUMULO_HOME");
    String zkHome = System.getenv("ZOOKEEPER_HOME");
    
    if (acuHome == null || zkHome == null) {
      throw new RuntimeException("ACCUMULO or ZOOKEEPER home not set!");
    }
    
    String retval = null;
    
    File zkLib = new File(zkHome);
    String[] files = zkLib.list();
    for (int i = 0; i < files.length; i++) {
      String f = files[i];
      if (f.matches("^zookeeper-.+jar$")) {
        if (retval == null) {
          retval = String.format("%s/%s", zkLib.getAbsolutePath(), f);
        } else {
          retval += String.format(",%s/%s", zkLib.getAbsolutePath(), f);
        }
      }
    }
    
    File libdir = new File(acuHome + "/lib");
    for (String jar : "accumulo-core accumulo-server-base accumulo-fate accumulo-trace libthrift".split(" ")) {
      retval += String.format(",%s/%s.jar", libdir.getAbsolutePath(), jar);
    }
    
    return retval;
  }
}
