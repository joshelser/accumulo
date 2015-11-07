/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.iteratortest;

import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * An interface that accepts some input for testing a {@link SortedKeyValueIterator}, runs the specific implementation
 * of the test and returns the outcome from that iterator.
 */
public interface IteratorTestCase {

  /**
   * Run the implementation's test against the given input.
   *
   * @param testInput The input to test.
   * @return The output of the test with the input.
   */
  IteratorTestOutput test(IteratorTestInput testInput);

}