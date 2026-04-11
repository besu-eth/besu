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
package org.hyperledger.besu.tests.acceptance.dsl.condition.bft;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.tests.acceptance.dsl.condition.Condition;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.Node;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.bft.BftTransactions;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

public class BftConditions {

  private final BftTransactions bft;

  public BftConditions(final BftTransactions bft) {
    this.bft = bft;
  }

  public List<BesuNode> validators(final BesuNode[] nodes) {
    final Comparator<BesuNode> compareByAddress =
        Comparator.comparing(node -> node.getAddress().getBytes());
    List<BesuNode> besuNodes = Arrays.asList(nodes);
    besuNodes.sort(compareByAddress);
    return besuNodes;
  }

  public ExpectValidators validatorsEqual(final BesuNode... validators) {
    return new ExpectValidators(bft, validatorAddresses(validators));
  }

  private Address[] validatorAddresses(final BesuNode[] validators) {
    return Arrays.stream(validators).map(BesuNode::getAddress).sorted().toArray(Address[]::new);
  }

  public Condition awaitValidatorSetChange(final Node node) {
    return new AwaitValidatorSetChange(node.execute(bft.createGetValidators("latest")), bft);
  }

  public Condition noProposals() {
    return new ExpectProposals(bft, ImmutableMap.of());
  }

  public PendingVotesConfig pendingVotesEqual() {
    return new PendingVotesConfig(bft);
  }

  public static class PendingVotesConfig {
    private final Map<BesuNode, Boolean> proposals = new HashMap<>();
    private final BftTransactions bft;

    private PendingVotesConfig(final BftTransactions bft) {
      this.bft = bft;
    }

    public PendingVotesConfig addProposal(final BesuNode node) {
      proposals.put(node, true);
      return this;
    }

    public PendingVotesConfig removeProposal(final BesuNode node) {
      proposals.put(node, false);
      return this;
    }

    public Condition build() {
      final Map<Address, Boolean> proposalsAsAddress =
          this.proposals.entrySet().stream()
              .collect(Collectors.toMap(p -> p.getKey().getAddress(), Map.Entry::getValue));
      return new ExpectProposals(bft, proposalsAsAddress);
    }
  }
}
