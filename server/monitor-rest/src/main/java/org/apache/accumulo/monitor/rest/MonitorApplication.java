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
package org.apache.accumulo.monitor.rest;

import io.dropwizard.Application;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.health.AccumuloHealthCheck;
import org.apache.accumulo.monitor.rest.resources.GarbageCollectorResource;
import org.apache.accumulo.monitor.rest.resources.MasterResource;
import org.apache.accumulo.monitor.rest.resources.ProblemsResource;
import org.apache.accumulo.monitor.rest.resources.ReplicationResource;
import org.apache.accumulo.monitor.rest.resources.StatisticsOverTimeResource;
import org.apache.accumulo.monitor.rest.resources.StatisticsResource;
import org.apache.accumulo.monitor.rest.resources.TablesResource;
import org.apache.accumulo.monitor.rest.resources.TabletServerResource;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;

import com.google.common.net.HostAndPort;

/**
 * 
 */
public class MonitorApplication extends Application<MonitorConfiguration> {
  private static final Logger log = Logger.getLogger(MonitorApplication.class);

  @Override
  public void initialize(Bootstrap<MonitorConfiguration> arg0) {
    // noop
    
  }

  @Override
  public void run(MonitorConfiguration conf, Environment env) throws Exception {
    final int port = getApplicationHttpPort(conf);
    Instance instance = HdfsZooInstance.getInstance();
    Monitor.setInstance(instance);
    ServerConfigurationFactory serverConf = new ServerConfigurationFactory(instance);
    Monitor.setConfiguration(serverConf);

    // need to regularly fetch data so plot data is updated
    new Daemon(new LoggingRunnable(log, new Runnable() {

      @Override
      public void run() {
        while (true) {
          try {
            Monitor.fetchData();
          } catch (Exception e) {
            log.warn(e.getMessage(), e);
          }

          UtilWaitThread.sleep(333);
        }

      }
    }), "Data fetcher").start();

    final VolumeManager fs = VolumeManagerImpl.get();
    Accumulo.init(fs, serverConf, "monitor-rest");

    // TODO find a way to pull the address from jersey
    String hostname = null;

    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      log.info("Could not calculate hostname", e);
    }

    if (null != hostname) {
      if (port > 0) {
        advertiseHttpAddress(instance, hostname, port);
      } else {
        log.info("Could not advertise HTTP address in ZooKeeper");
      }
  
      // TODO Log messages get forwarded to this service, but aren't being made available to the LogResource
      // LogService.startLogListener(Monitor.getSystemConfiguration(), instance.getInstanceID(), hostname);
    } else {
      log.warn("Not starting log4j listener as we could not determine address to use");
      
    }
    
    // Add health checks
    env.healthChecks().register("accumulo", new AccumuloHealthCheck(instance));

    // Register the resources with Jersey
    env.jersey().register(new TablesResource());
    env.jersey().register(new StatisticsResource());
    env.jersey().register(new StatisticsOverTimeResource());
    env.jersey().register(new ProblemsResource());
    env.jersey().register(new GarbageCollectorResource());
    env.jersey().register(new MasterResource());
    env.jersey().register(new ReplicationResource());
    env.jersey().register(new TabletServerResource());

    // TODO Log messages get forwarded to this service, but aren't being made available to the LogResource
    // env.jersey().register(new LogResource());
  }

  protected void advertiseHttpAddress(Instance instance, String hostname, int port) {
    try {
      hostname = InetAddress.getLocalHost().getHostName();

      log.debug("Using " + hostname + " to advertise monitor location in ZooKeeper");

      String monitorAddress = HostAndPort.fromParts(hostname, port).toString();

      ZooReaderWriter.getInstance().putPersistentData(ZooUtil.getRoot(instance) + Constants.ZMONITOR_HTTP_ADDR, monitorAddress.getBytes(StandardCharsets.UTF_8),
          NodeExistsPolicy.OVERWRITE);
      log.info("Set monitor address in zookeeper to " + monitorAddress);
    } catch (Exception ex) {
      log.error("Unable to set monitor HTTP address in zookeeper", ex);
    }
  }

  protected int getApplicationHttpPort(MonitorConfiguration conf) {
    ServerFactory serverFactory = conf.getServerFactory();
    if (serverFactory instanceof DefaultServerFactory) {
      DefaultServerFactory defaultServerFactory = (DefaultServerFactory) serverFactory;
      List<ConnectorFactory> connectors = defaultServerFactory.getApplicationConnectors();
      if (connectors.isEmpty()) {
        return 0;
      }

      ConnectorFactory connector = connectors.get(0);
      if (connector instanceof HttpConnectorFactory) {
        return ((HttpConnectorFactory) connector).getPort();
      }
    }

    return 0;
  }

  @Override
  public String getName() {
    return "Accumulo Monitor";
  }

  public static void main(String[] args) throws Exception {
    new MonitorApplication().run(args);
  }
}
