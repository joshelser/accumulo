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
package org.apache.accumulo.randomwalk.error;

import org.apache.accumulo.randomwalk.Node;

import com.google.common.base.Preconditions;

/**
 * Message to report an error which occurred in a Randomwalk test {@link Node}.
 * 
 * Contains the Class which created this error and a message to describe what happened.
 */
public class ErrorReport {

  protected Class<? extends Node> nodeClass;
  protected String message;
  
  /**
   * An error message from a given node class
   * @param nodeClass
   * @param message
   */
  public ErrorReport(Class<? extends Node> nodeClass, String message) {
    Preconditions.checkNotNull(nodeClass);
    Preconditions.checkNotNull(message);
    
    this.nodeClass = nodeClass;
    this.message = message;
  }
  
  public Class<?> getNodeClass() {
    return nodeClass;
  }
  
  public String getMessage() {
    return message;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof ErrorReport) {
      ErrorReport other = (ErrorReport) o;
      return nodeClass.equals(other.getNodeClass()) && message.equals(other.getMessage());
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    return nodeClass.getCanonicalName() + ": '" + message + "'";
  }
}
