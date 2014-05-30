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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.tserver.TabletServer;
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
  private ExecutorService pool;
  private int actualZooKeeperPort;

  public AccumuloMicroCluster(AccumuloMicroConfig cfg) {
    this.cfg = cfg;
    this.pool = Executors.newFixedThreadPool(3);
  }

  @Override
  public void start() throws IOException, InterruptedException {
    File dataDir = Files.createTempDir();
    zk = new ZooKeeperServerMain();
    final ServerConfig zkCfg = new ServerConfig();
    actualZooKeeperPort = cfg.getZooKeeperPort();
    if (0 == actualZooKeeperPort) {
      actualZooKeeperPort = PortUtils.getRandomFreePort();
    }
    zkCfg.parse(new String[] {Integer.toString(actualZooKeeperPort), dataDir.getAbsolutePath()});

    Runnable zkRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          zk.runFromConfig(zkCfg);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };

    pool.execute(zkRunnable);

    SiteConfiguration siteConf = SiteConfiguration.getInstance(DefaultConfiguration.getDefaultConfiguration());
    siteConf.set(Property.INSTANCE_VOLUMES, cfg.getDir().toURI().toString());
    siteConf.set(Property.INSTANCE_ZK_HOST, "127.0.0.1:" + actualZooKeeperPort);
    siteConf.set(Property.MASTER_CLIENTPORT, "0");
    siteConf.set(Property.TSERV_CLIENTPORT, "0");
    siteConf.set(Property.GC_PORT, "0");
    siteConf.set(Property.TRACE_PORT, "0");

    Initialize.main(new String[] {"--instance-name", cfg.getInstanceName(), "--clear-instance-name", "--password", cfg.getRootPassword()});

    pool.execute(new Runnable() {
      @Override
      public void run() {
        try {
          Master.main(new String[0]);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    pool.execute(new Runnable() {
      @Override
      public void run() {
        try {
          TabletServer.main(new String[0]);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  @Override
  public String getInstanceName() {
    return cfg.getInstanceName();
  }

  @Override
  public String getZooKeepers() {
    return "127.0.0.1:" + actualZooKeeperPort;
  }

  @Override
  public void stop() throws IOException, InterruptedException {
    pool.shutdownNow();
  }

  @Override
  public AccumuloMicroConfig getConfig() {
    return cfg;
  }

  @Override
  public Connector getConnector(String user, String passwd) throws AccumuloException, AccumuloSecurityException {
    ZooKeeperInstance inst = new ZooKeeperInstance(getInstanceName(), getZooKeepers());
    return inst.getConnector(user, new PasswordToken(passwd));
  }

  @Override
  public ClientConfiguration getClientConfig() {
    return null;
  }

}
