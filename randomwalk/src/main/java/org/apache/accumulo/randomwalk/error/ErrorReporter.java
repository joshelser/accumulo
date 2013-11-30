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

import java.util.Map.Entry;

/**
 * Interface to allow multiple backing stores to track {@link ErrorReport} occurrences.
 * 
 */
public interface ErrorReporter {

  /**
   * Log that an {@link ErrorReport} occurred.
   * @param error
   */
  public void addError(ErrorReport error);
  
  /**
   * Fetch the {@link ErrorReport}s seen and the number of occurrences in which we've seen the error 
   * @return
   */
  public Iterable<Entry<ErrorReport,Long>> report();
}
