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
package org.apache.accumulo.tserver;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.security.VisibilityConstraint;
import org.apache.accumulo.tserver.constraints.ActiveConstraints;
import org.apache.accumulo.tserver.constraints.ConstraintChecker;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class ActiveConstraintsTest {

  private ConstraintChecker constraintChecker;
  private Text tableId;
  private KeyExtent extent;

  @Before
  public void setupMocks() {
    tableId = new Text("1");
    constraintChecker = createMock(ConstraintChecker.class);
    extent = new KeyExtent(tableId, null, null);
  }

  @Test
  public void testNoTablets() {
    ActiveConstraints ac = new ActiveConstraints(tableId, Collections.<Tablet> emptyList());
    assertEquals(Collections.emptySet(), ac.getMinimumConstraints());
  }

  @Test
  public void testTabletsForOtherTable() {
    Tablet tablet = createMock(Tablet.class);
    ActiveConstraints ac = new ActiveConstraints(new Text("2"), Collections.singletonList(tablet));

    expect(tablet.getExtent()).andReturn(extent);

    replay(tablet, constraintChecker);

    assertEquals(Collections.emptySet(), ac.getMinimumConstraints());

    verify(tablet, constraintChecker);
  }

  @Test
  public void testSingleTablet() {
    Tablet tablet = createMock(Tablet.class);
    ActiveConstraints ac = new ActiveConstraints(tableId, Collections.singletonList(tablet));
    List<Constraint> constraints = Arrays.asList(new VisibilityConstraint(), new DefaultKeySizeConstraint());

    expect(tablet.getExtent()).andReturn(extent);
    expect(tablet.getConstraintChecker()).andReturn(constraintChecker);
    expect(constraintChecker.getConstraints()).andReturn(constraints);

    replay(tablet, constraintChecker);

    Set<String> minConstraints = ac.getMinimumConstraints();

    verify(tablet, constraintChecker);

    for (Constraint c : constraints) {
      assertTrue("Expected to find " + c + " in minimum set", minConstraints.remove(c.getClass().getName()));
    }
    assertTrue("Found extra constraints: " + minConstraints, minConstraints.isEmpty());
  }

  @Test
  public void testMultipleTablets() {
    Tablet tablet1 = createMock(Tablet.class), tablet2 = createMock(Tablet.class);
    ActiveConstraints ac = new ActiveConstraints(tableId, Arrays.asList(tablet1, tablet2));
    List<Constraint> constraints = Arrays.asList(new VisibilityConstraint(), new DefaultKeySizeConstraint());

    expect(tablet1.getExtent()).andReturn(extent);
    expect(tablet2.getExtent()).andReturn(extent);
    expect(tablet1.getConstraintChecker()).andReturn(constraintChecker);
    expect(tablet2.getConstraintChecker()).andReturn(constraintChecker);
    expect(constraintChecker.getConstraints()).andReturn(constraints).times(2);

    replay(tablet1, tablet2, constraintChecker);

    Set<String> minConstraints = ac.getMinimumConstraints();

    verify(tablet1, tablet2, constraintChecker);

    for (Constraint c : constraints) {
      assertTrue("Expected to find " + c + " in minimum set", minConstraints.remove(c.getClass().getName()));
    }
    assertTrue("Found extra constraints: " + minConstraints, minConstraints.isEmpty());
  }

  @Test
  public void testMinimumMultipleTablets() {
    Tablet tablet1 = createMock(Tablet.class), tablet2 = createMock(Tablet.class);
    ActiveConstraints ac = new ActiveConstraints(tableId, Arrays.asList(tablet1, tablet2));
    List<Constraint> constraints1 = Arrays.asList(new VisibilityConstraint(), new DefaultKeySizeConstraint()), constraints2 = Arrays
        .<Constraint> asList(new VisibilityConstraint());
    ConstraintChecker constraintChecker2 = createMock(ConstraintChecker.class);

    expect(tablet1.getExtent()).andReturn(extent);
    expect(tablet2.getExtent()).andReturn(extent);
    expect(tablet1.getConstraintChecker()).andReturn(constraintChecker);
    expect(tablet2.getConstraintChecker()).andReturn(constraintChecker2);
    expect(constraintChecker.getConstraints()).andReturn(constraints1);
    expect(constraintChecker2.getConstraints()).andReturn(constraints2);

    replay(tablet1, tablet2, constraintChecker, constraintChecker2);

    Set<String> minConstraints = ac.getMinimumConstraints();

    verify(tablet1, tablet2, constraintChecker, constraintChecker2);

    for (Constraint c : constraints2) {
      assertTrue("Expected to find " + c + " in minimum set", minConstraints.remove(c.getClass().getName()));
    }
    assertTrue("Found extra constraints: " + minConstraints, minConstraints.isEmpty());
  }

  @Test
  public void testMinimumMultipleTabletsWithExtraTablets() {
    Tablet tablet1 = createMock(Tablet.class), tablet2 = createMock(Tablet.class), tablet3 = createMock(Tablet.class);
    KeyExtent otherExtent = new KeyExtent(new Text("5"), null, null);
    ActiveConstraints ac = new ActiveConstraints(tableId, Arrays.asList(tablet1, tablet3, tablet2, tablet3));
    List<Constraint> constraints1 = Arrays.asList(new VisibilityConstraint(), new DefaultKeySizeConstraint()), constraints2 = Arrays
        .<Constraint> asList(new VisibilityConstraint());
    ConstraintChecker constraintChecker2 = createMock(ConstraintChecker.class);

    expect(tablet1.getExtent()).andReturn(extent);
    expect(tablet2.getExtent()).andReturn(extent);
    expect(tablet3.getExtent()).andReturn(otherExtent).times(2);
    expect(tablet1.getConstraintChecker()).andReturn(constraintChecker);
    expect(tablet2.getConstraintChecker()).andReturn(constraintChecker2);
    expect(constraintChecker.getConstraints()).andReturn(constraints1);
    expect(constraintChecker2.getConstraints()).andReturn(constraints2);

    replay(tablet1, tablet2, tablet3, constraintChecker, constraintChecker2);

    Set<String> minConstraints = ac.getMinimumConstraints();

    verify(tablet1, tablet2, tablet3, constraintChecker, constraintChecker2);

    for (Constraint c : constraints2) {
      assertTrue("Expected to find " + c + " in minimum set", minConstraints.remove(c.getClass().getName()));
    }
    assertTrue("Found extra constraints: " + minConstraints, minConstraints.isEmpty());
  }
}
