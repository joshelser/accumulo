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
package org.apache.accumulo.monitor.wizard.resources;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.monitor.Monitor;

/**
 * GarbageCollector metrics
 */
@Path("/gc")
@Produces(MediaType.APPLICATION_JSON)
public class GarbageCollectorResource {

  public GarbageCollectorStatus getStatus() {
    return new GarbageCollectorStatus(Monitor.getGcStatus());
  }

  @Path("/last")
  public GarbageCollectorCycle getLastCycle() {
    GCStatus status = Monitor.getGcStatus();
    return new GarbageCollectorCycle(status.last);
  }

  @Path("/current")
  public GarbageCollectorCycle getCurrentCycle() {
    GCStatus status = Monitor.getGcStatus();
    return new GarbageCollectorCycle(status.current);
  }

  @Path("/wals/last")
  public GarbageCollectorCycle getLastWalCycle() {
    GCStatus status = Monitor.getGcStatus();
    return new GarbageCollectorCycle(status.lastLog);
  }

  @Path("/wals/current")
  public GarbageCollectorCycle getCurrentWalCycle() {
    GCStatus status = Monitor.getGcStatus();
    return new GarbageCollectorCycle(status.currentLog);
  }
}
