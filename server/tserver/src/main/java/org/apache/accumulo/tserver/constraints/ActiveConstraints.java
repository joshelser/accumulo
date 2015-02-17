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
package org.apache.accumulo.tserver.constraints;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.tserver.Tablet;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveConstraints {
  private static final Logger log = LoggerFactory.getLogger(ActiveConstraints.class);

  private Text tableId;
  private Collection<Tablet> tablets;

  public ActiveConstraints(Text tableId, Collection<Tablet> tablets) {
    this.tableId = tableId;
    this.tablets = tablets;
  }

  /**
   * Compute the minimum set of {@link Constraint}s loaded by the {@link Tablet}s for a table.
   */
  public Set<String> getMinimumConstraints() {
    Set<String> observedConstraints = null;
    for (Tablet tablet : tablets) {
      // Only inspect tablets for the table in question
      if (tableId.equals(tablet.getExtent().getTableId())) {
        log.info("Inspecting {}", tablet);
        ConstraintChecker constraintChecker = tablet.getConstraintChecker();
        List<Constraint> tabletConstraints = constraintChecker.getConstraints();

        // Initial state
        if (null == observedConstraints) {
          observedConstraints = new HashSet<String>();
          for (Constraint c : tabletConstraints) {
            observedConstraints.add(c.getClass().getName());
          }
          log.info("Initial observed constraints {}", observedConstraints);
        } else {
          // Get the intersection between the previous observed constraints and the constraints for the current tablet
          Set<String> tabletConstraintClassNames = new HashSet<String>();
          for (Constraint constraint : tabletConstraints) {
            tabletConstraintClassNames.add(constraint.getClass().getName());
          }
          log.info("Constraint classes for tablet {}", tabletConstraintClassNames);
          observedConstraints.retainAll(tabletConstraintClassNames);
        }
      } else {
        log.info("Ignoring tablet for {}", tablet.getExtent().getTableId());
      }
    }

    if (null == observedConstraints) {
      return Collections.emptySet();
    }

    return observedConstraints;
  }
}
