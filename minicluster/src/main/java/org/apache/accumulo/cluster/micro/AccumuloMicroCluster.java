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

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;

import com.google.common.io.Files;

/**
 * 
 */
public class AccumuloMicroCluster implements AccumuloCluster {
  private static final Logger log = Logger.getLogger(AccumuloMicroCluster.class);

  private final AccumuloMicroConfig cfg;

  private ZooKeeperServerMain zk;
  private Thread zkThread;

  public AccumuloMicroCluster(AccumuloMicroConfig cfg) {
    this.cfg = cfg;
  }

  @Override
  public void start() throws IOException, InterruptedException {
    File dataDir = Files.createTempDir();
    zk = new ZooKeeperServerMain();
    final ServerConfig zkCfg = new ServerConfig();
    zkCfg.parse(new String[] {Integer.toString(cfg.getZooKeeperPort()), dataDir.getAbsolutePath()});

    zkThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          zk.runFromConfig(zkCfg);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });

    zkThread.start();
  }

  @Override
  public String getInstanceName() {
    return cfg.getInstanceName();
  }

  @Override
  public String getZooKeepers() {
    return "localhost:" + cfg.getZooKeeperPort();
  }

  @Override
  public void stop() throws IOException, InterruptedException {}

  @Override
  public AccumuloMicroConfig getConfig() {
    return cfg;
  }

  @Override
  public Connector getConnector(String user, String passwd) throws AccumuloException, AccumuloSecurityException {
    return null;
  }

  @Override
  public ClientConfiguration getClientConfig() {
    return null;
  }

}
