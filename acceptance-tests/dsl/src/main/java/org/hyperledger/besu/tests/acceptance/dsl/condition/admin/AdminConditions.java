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
package org.hyperledger.besu.tests.acceptance.dsl.condition.admin;

import static org.assertj.core.api.Assertions.fail;

import org.hyperledger.besu.tests.acceptance.dsl.condition.Condition;
import org.hyperledger.besu.tests.acceptance.dsl.node.Node;
import org.hyperledger.besu.tests.acceptance.dsl.node.RunnableNode;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.admin.AdminNodeInfoTransaction;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.admin.AdminTransactions;

import java.net.URI;

public class AdminConditions {

  private final AdminTransactions admin;

  public AdminConditions(final AdminTransactions admin) {
    this.admin = admin;
  }

  public Condition addPeer(final Node addingPeer) {
    return new ExpectPeerAdded(admin.addPeer(enodeUrl(addingPeer)));
  }

  public Condition hasPeer(final Node peer) {
    return new ExpectHasPeer(nodeId(peer), admin.listPeers());
  }

  public Condition doesNotHavePeer(final Node peer) {
    return new ExpectNotHavePeer(nodeId(peer), admin.listPeers());
  }

  public AdminNodeInfoTransaction nodeInfo() {
    return admin.nodeInfo();
  }

  private URI enodeUrl(final Node node) {
    if (!(node instanceof RunnableNode)) {
      fail("A RunnableNode instance is required");
    }

    return ((RunnableNode) node).enodeUrl();
  }

  private String nodeId(final Node node) {
    if (!(node instanceof RunnableNode)) {
      fail("A RunnableNode instance is required");
    }

    return "0x" + ((RunnableNode) node).getNodeId();
  }
}
