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

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 */
public class GarbageCollectorStatus {

  protected GarbageCollectorCycle last, current, lastWals, currentWals;

  public GarbageCollectorStatus() {}

  public GarbageCollectorStatus(GCStatus status) {
    if (null != status) {
      last = new GarbageCollectorCycle(status.last);
      current = new GarbageCollectorCycle(status.current);
      lastWals = new GarbageCollectorCycle(status.lastLog);
      currentWals = new GarbageCollectorCycle(status.currentLog);
    } else {
      last = new GarbageCollectorCycle();
      current = new GarbageCollectorCycle();
      lastWals = new GarbageCollectorCycle();
      currentWals = new GarbageCollectorCycle();
    }
  }

  @JsonProperty("lastCycle")
  public GarbageCollectorCycle getLastCycle() {
    return last;
  }

  @JsonProperty("lastCycle")
  public void setLastCycle(GarbageCollectorCycle last) {
    this.last = last;
  }

  @JsonProperty("currentCycle")
  public GarbageCollectorCycle getCurrentCycle() {
    return current;
  }

  @JsonProperty("currentCycle")
  public void setCurrentCycle(GarbageCollectorCycle current) {
    this.current = current;
  }

  @JsonProperty("lastWalCycle")
  public GarbageCollectorCycle getLastWalCycle() {
    return lastWals;
  }

  @JsonProperty("lastWalCycle")
  public void setLastWalCycle(GarbageCollectorCycle lastWals) {
    this.lastWals = lastWals;
  }

  @JsonProperty("currentWalCycle")
  public GarbageCollectorCycle getCurrentWalCycle() {
    return currentWals;
  }

  @JsonProperty("currentWalCycle")
  public void setCurrentWalCycle(GarbageCollectorCycle currentWals) {
    this.currentWals = currentWals;
  }
}
