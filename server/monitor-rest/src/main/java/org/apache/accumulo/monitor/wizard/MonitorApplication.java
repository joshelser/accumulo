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
package org.apache.accumulo.monitor.wizard;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.wizard.resources.GarbageCollectorResource;
import org.apache.accumulo.monitor.wizard.resources.ProblemsResource;
import org.apache.accumulo.monitor.wizard.resources.StatisticsOverTimeResource;
import org.apache.accumulo.monitor.wizard.resources.StatisticsResource;
import org.apache.accumulo.monitor.wizard.resources.TablesResource;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.log4j.Logger;

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
    Instance instance = HdfsZooInstance.getInstance();
    Monitor.setInstance(instance);
    Monitor.setConfiguration(new ServerConfiguration(instance));

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

    // Add health checks
    env.healthChecks().register("accumulo", new AccumuloHealthCheck(instance));

    // Register the resources with Jersey
    env.jersey().register(new TablesResource());
    env.jersey().register(new StatisticsResource());
    env.jersey().register(new StatisticsOverTimeResource());
    env.jersey().register(new ProblemsResource());
    env.jersey().register(new GarbageCollectorResource());
  }

  @Override
  public String getName() {
    return "Accumulo Monitor";
  }

  public static void main(String[] args) throws Exception {
    new MonitorApplication().run(args);
  }
}
