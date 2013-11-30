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

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * In-memory implementation to track the occurrences of {@link ErrorReport}s
 */
public class InMemoryErrorReporter implements ErrorReporter {
  protected HashMap<ErrorReport,AtomicLong> errors;
  
  public InMemoryErrorReporter() {
    errors = new HashMap<ErrorReport,AtomicLong>();
  }

  @Override
  public void addError(ErrorReport error) {
    Preconditions.checkNotNull(error);
    
    synchronized(errors) {
      if (errors.containsKey(error)) {
        errors.get(error).incrementAndGet();
      } else {
        errors.put(error, new AtomicLong(1l));
      }
    }
  }
  
  private static class Transform implements Function<Entry<ErrorReport,AtomicLong>,Entry<ErrorReport,Long>> {
    @Override
    public Entry<ErrorReport,Long> apply(Entry<ErrorReport,AtomicLong> input) {
      return Maps.immutableEntry(input.getKey(), input.getValue().longValue());
    }
  }
  
  private static Transform TRANSFORM_INSTANCE = new Transform();

  @Override
  public Iterable<Entry<ErrorReport,Long>> report() {
    HashMap<ErrorReport,AtomicLong> copy;
    
    // Get a view of the errors
    synchronized (errors) {
      copy = new HashMap<ErrorReport,AtomicLong>(errors);
    }
    
    return Iterables.transform(copy.entrySet(), TRANSFORM_INSTANCE);
  }
  
}
