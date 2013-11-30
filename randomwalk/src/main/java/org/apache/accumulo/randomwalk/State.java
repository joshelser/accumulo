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
package org.apache.accumulo.randomwalk;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.accumulo.randomwalk.error.ErrorReporter;
import org.apache.log4j.Logger;

public class State {
  
  private static final Logger log = Logger.getLogger(State.class);
  protected HashMap<String,Object> stateMap = new HashMap<String,Object>();
  protected Properties props;
  protected int numVisits = 0;
  protected int maxVisits = Integer.MAX_VALUE;
  protected ErrorReporter errorReporter = null;
  
  /**
   * Loads the State with the given properties, typically loaded from the randomwalk.conf
   * @param props
   */
  public State(Properties props, ErrorReporter errorReporter) {
    this.props = props;
    this.errorReporter = errorReporter;
  }
  
  public void setMaxVisits(int num) {
    maxVisits = num;
  }
  
  public void visitedNode() throws Exception {
    numVisits++;
    if (numVisits > maxVisits) {
      log.debug("Visited max number (" + maxVisits + ") of nodes");
      throw new Exception("Visited max number (" + maxVisits + ") of nodes");
    }
  }
  
  public String getPid() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }
  
  public void set(String key, Object value) {
    stateMap.put(key, value);
  }
  
  public boolean containsKey(String key) {
    return stateMap.containsKey(key);
  }
  
  public Object get(String key) {
    if (stateMap.containsKey(key) == false) {
      throw new NoSuchElementException("State does not contain " + key);
    }
    return stateMap.get(key);
  }
  
  public HashMap<String,Object> getMap() {
    return stateMap;
  }
  
  public ErrorReporter getErrorReporter() {
    return errorReporter;
  }
  
  /**
   * 
   * @return a copy of Properties, so accidental changes don't affect the framework
   */
  public Properties getProperties() {
    return new Properties(props);
  }
  
  public String getString(String key) {
    return (String) get(key);
  }
  
  public Long getLong(String key) {
    return (Long) get(key);
  }
  
  public String getProperty(String key) {
    return props.getProperty(key);
  }
}
