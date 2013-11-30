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

import org.apache.log4j.Logger;

/**
 * Setup and teardown methods which can be assigned to a {@link Module}
 * 
 * If certain preconditions need to be true, the setUp() method can perform actions before any Nodes in
 * the Module will be invoked.
 * 
 * Likewise, the tearDown() method can clean up after the Nodes in the Module to ensure the original state
 * of the system is preserved.
 */
public abstract class Fixture {
  
  protected final Logger log = Logger.getLogger(this.getClass());
  
  public abstract void setUp(State state) throws Exception;
  
  public abstract void tearDown(State state) throws Exception;
}
