/*
 * Copyright ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.permissioning.node;

import org.hyperledger.besu.plugin.data.EnodeURL;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class NodeAllowlistUpdatedEvent {

  private final List<EnodeURL> addedNodes;
  private final List<? extends EnodeURL> removedNodes;

  public NodeAllowlistUpdatedEvent(
      final List<EnodeURL> addedNodes, final List<? extends EnodeURL> removedNodes) {
    this.addedNodes = addedNodes != null ? addedNodes : Collections.emptyList();
    this.removedNodes = removedNodes != null ? removedNodes : Collections.emptyList();
  }

  public List<EnodeURL> getAddedNodes() {
    return addedNodes;
  }

  public List<? extends EnodeURL> getRemovedNodes() {
    return removedNodes;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeAllowlistUpdatedEvent that = (NodeAllowlistUpdatedEvent) o;
    return Objects.equals(addedNodes, that.addedNodes)
        && Objects.equals(removedNodes, that.removedNodes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(addedNodes, removedNodes);
  }
}
