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

import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.monitor.util.celltypes.CompactionsType;
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
  protected CompactionsType scans, minc, majc;

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

  @JsonProperty("scans")
  public CompactionsType getScans() {
    return scans;
  }

  @JsonProperty
  public void setScans(CompactionsType scans) {
    this.scans = scans;
  }

  @JsonProperty("minorCompactions")
  public CompactionsType getMinc() {
    return minc;
  }

  @JsonProperty
  public void setMinc(CompactionsType minc) {
    this.minc = minc;
  }

  @JsonProperty("majorCompactions")
  public CompactionsType getMajc() {
    return majc;
  }
  
  @JsonProperty
  public void setMajc(CompactionsType majc) {
    this.majc = majc;
  }
}
