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

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.security.SystemCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.health.HealthCheck;

/**
 * 
 */
public class AccumuloHealthCheck extends HealthCheck {
  private static final Logger log = LoggerFactory.getLogger(AccumuloHealthCheck.class);

  private Instance inst;
  private ExecutorService executor;

  public AccumuloHealthCheck(Instance inst) {
    this.inst = inst;
    executor = Executors.newSingleThreadExecutor();
  }

  @Override
  protected Result check() throws Exception {
    if (inst.getMasterLocations().isEmpty()) {
      return Result.unhealthy("No Masters running");
    }

    List<TabletServerStatus> tservers = Monitor.getMmi().getTServerInfo();
    if (tservers.isEmpty()) {
      return Result.unhealthy("No TServers running");
    }

    if (Monitor.getMmi().getDeadTabletServersSize() * 2 >= tservers.size()) {
      return Result.unhealthy("Less than half of configured TServers are alive");
    }

    FutureTask<Boolean> future = new FutureTask<Boolean>(new Callable<Boolean>() {
      @Override
      public Boolean call() throws InterruptedException {
        try {
          SystemCredentials creds = SystemCredentials.get();
          Connector conn = inst.getConnector(creds.getPrincipal(), creds.getToken());
          for (@SuppressWarnings("unused") Entry<Key,Value> entry : conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
            // noop
          }
          return true;
        } catch (Exception e) {
          log.debug("Could not read metadata table", e);
        }
        return false;
      }
    });
    executor.execute(future);

    try {
      if (!future.get(5, TimeUnit.SECONDS)) {
        return Result.unhealthy("Could not read Accumulo metadata table");
      }
    } catch (Exception e) {
      return Result.unhealthy("Could not read Accumulo metadata table");
    }

    return Result.healthy();
  }

}
