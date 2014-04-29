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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.cluster.AccumuloConfig;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;

/**
 * 
 */
public class AccumuloMicroConfig implements AccumuloConfig {
  private static final AtomicInteger MICRO_CLUSTER_COUNT = new AtomicInteger(0);

  private String instanceName;
  private int numTservers = 1;
  private Map<String,String> siteConfig;
  private int zooKeeperPort = 0;
  private String password;

  private String[] nativePathItems = null;

  public AccumuloMicroConfig(String password) {
    this(password, "microinstance" + MICRO_CLUSTER_COUNT.incrementAndGet());
  }
  
  public AccumuloMicroConfig(String password, String instanceName) {
    this.password = password;
    this.instanceName = instanceName;
  }

  @Override
  public AccumuloConfig setNumTservers(int numTservers) {
    this.numTservers = numTservers;
    return this;
  }

  @Override
  public AccumuloConfig setInstanceName(String instanceName) {
    this.instanceName = instanceName;
    return this;
  }

  @Override
  public AccumuloConfig setSiteConfig(Map<String,String> siteConfig) {
    this.siteConfig = siteConfig;
    return this;
  }

  @Override
  public AccumuloConfig setZooKeeperPort(int zooKeeperPort) {
    this.zooKeeperPort = zooKeeperPort;
    return this;
  }

  @Override
  public AccumuloConfig setMemory(ServerType serverType, long memory, MemoryUnit memoryUnit) {
    throw new UnsupportedOperationException("AccumuloMicroCluster runs in the invoking processes JVM");
  }

  @Override
  public AccumuloConfig setDefaultMemory(long memory, MemoryUnit memoryUnit) {
    throw new UnsupportedOperationException("AccumuloMicroCluster runs in the invoking processes JVM");
  }

  @Override
  public Map<String,String> getSiteConfig() {
    return siteConfig;
  }

  @Override
  public int getZooKeeperPort() {
    return zooKeeperPort;
  }

  @Override
  public String getInstanceName() {
    return instanceName;
  }

  @Override
  public long getMemory(ServerType serverType) {
    throw new UnsupportedOperationException("AccumuloMicroCluster runs in the invoking processes JVM");
  }

  @Override
  public long getDefaultMemory() {
    throw new UnsupportedOperationException("AccumuloMicroCluster runs in the invoking processes JVM");
  }

  @Override
  public String getRootPassword() {
    return password;
  }

  @Override
  public int getNumTservers() {
    return numTservers;
  }

  @Override
  public String[] getNativeLibPaths() {
    return this.nativePathItems == null ? new String[0] : this.nativePathItems;
  }

  @Override
  public AccumuloConfig setNativeLibPaths(String... nativePathItems) {
    this.nativePathItems = nativePathItems;
    return this;
  }

  @Override
  public AccumuloMicroCluster build() throws IOException {
    return new AccumuloMicroCluster(this);
  }

}
