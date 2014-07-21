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
package org.apache.accumulo.monitor.rest.model;

import org.apache.accumulo.core.master.state.tables.TableState;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 */
public class Table {
  protected String tableName, tableId;
  protected int tablets;
  protected TableState tableState;
  protected int offlineTables;
  protected long entries;
  protected long entriesInMemory;
  protected double ingest;
  protected double entriesRead, entriesReturned;
  protected long holdTime;
  protected int queuedScans, runningScans;
  protected int queuedMinc, runningMinc;
  protected int queuedMajc, runningMajc;

  public Table() {}

  @JsonProperty("tableName")
  public String getTableName() {
    return tableName;
  }

  @JsonProperty
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @JsonProperty("tableId")
  public String getTableId() {
    return tableId;
  }

  @JsonProperty
  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  @JsonProperty("tables")
  public int getTablets() {
    return tablets;
  }

  @JsonProperty
  public void setTablets(int tablets) {
    this.tablets = tablets;
  }

  @JsonProperty("tableState")
  public TableState getTableState() {
    return tableState;
  }

  @JsonProperty
  public void setTableState(TableState tableState) {
    this.tableState = tableState;
  }

  @JsonProperty("offlineTablets")
  public int getOfflineTables() {
    return offlineTables;
  }

  @JsonProperty
  public void setOfflineTables(int offlineTables) {
    this.offlineTables = offlineTables;
  }

  @JsonProperty("entries")
  public long getEntries() {
    return entries;
  }

  @JsonProperty
  public void setEntries(long entries) {
    this.entries = entries;
  }

  @JsonProperty("entriesInMemory")
  public long getEntriesInMemory() {
    return entriesInMemory;
  }

  @JsonProperty
  public void setEntriesInMemory(long entriesInMemory) {
    this.entriesInMemory = entriesInMemory;
  }

  @JsonProperty("ingest")
  public double getIngest() {
    return ingest;
  }

  @JsonProperty
  public void setIngest(double ingest) {
    this.ingest = ingest;
  }

  @JsonProperty("entriesRead")
  public double getEntriesRead() {
    return entriesRead;
  }

  @JsonProperty
  public void setEntriesRead(double entriesRead) {
    this.entriesRead = entriesRead;
  }

  @JsonProperty("entriesReturned")
  public double getEntriesReturned() {
    return entriesReturned;
  }

  @JsonProperty
  public void setEntriesReturned(double entriesReturned) {
    this.entriesReturned = entriesReturned;
  }

  @JsonProperty("holdTime")
  public double getHoldTime() {
    return holdTime;
  }

  @JsonProperty
  public void setHoldTime(long holdTime) {
    this.holdTime = holdTime;
  }

  @JsonProperty("runningScans")
  public int getRunningScans() {
    return runningScans;
  }

  @JsonProperty
  public void setRunningScans(int runningScans) {
    this.runningScans = runningScans;
  }

  @JsonProperty("queuedScans")
  public int getQueuedScans() {
    return queuedScans;
  }

  @JsonProperty
  public void setQueuedScans(int queuedScans) {
    this.queuedScans = queuedScans;
  }

  @JsonProperty("runningMinorCompactions")
  public int getRunningMinorCompactions() {
    return runningMinc;
  }

  @JsonProperty
  public void setRunningMinorCompactions(int runningMinorCompactions) {
    this.runningMinc = runningMinorCompactions;
  }

  @JsonProperty("queuedMinorCompactions")
  public int getQueuedMinorCompactions() {
    return queuedMinc;
  }

  @JsonProperty
  public void setQueuedMinorCompactions(int queuedMinorCompactions) {
    this.queuedMinc = queuedMinorCompactions;
  }

  @JsonProperty("runningMajorCompactions")
  public int getRunningMajorCompactions() {
    return runningMajc;
  }

  @JsonProperty
  public void setRunningMajorCompactions(int runningMajorCompactions) {
    this.runningMajc = runningMajorCompactions;
  }

  @JsonProperty("queuedMajorCompactions")
  public int getQueuedMajorCompactions() {
    return queuedMajc;
  }

  @JsonProperty
  public void setQueuedMajorCompactions(int queuedMajc) {
    this.queuedMajc = queuedMajc;
  }
}
