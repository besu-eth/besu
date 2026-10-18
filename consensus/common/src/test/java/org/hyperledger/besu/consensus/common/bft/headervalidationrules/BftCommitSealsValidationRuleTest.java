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
package org.hyperledger.besu.consensus.common.bft.headervalidationrules;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.common.bft.BftContextBuilder.setupContextWithValidators;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Util;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class BftCommitSealsValidationRuleTest {

  private final BlockHeader blockHeader = mock(BlockHeader.class);
  private final BftCommitSealsValidationRule commitSealsValidationRule =
      new BftCommitSealsValidationRule();

  @Test
  public void correctlyConstructedHeaderPassesValidation() {
    final List<NodeKey> committerNodeKeys =
        IntStream.range(0, 2).mapToObj(i -> NodeKeyUtils.generate()).collect(Collectors.toList());

    final List<Address> committerAddresses =
        committerNodeKeys.stream()
            .map(nodeKey -> Util.publicKeyToAddress(nodeKey.getPublicKey()))
            .sorted()
            .collect(Collectors.toList());

    final BftContext bftContext = setupContextWithValidators(committerAddresses);
    final ProtocolContext context =
        new ProtocolContext.Builder().withConsensusContext(bftContext).build();
    stubCommitters(bftContext, committerAddresses);

    assertThat(commitSealsValidationRule.validate(blockHeader, null, context)).isTrue();
  }

  @Test
  public void insufficientCommitSealsFailsValidation() {
    final NodeKey committerNodeKey = NodeKeyUtils.generate();
    final Address committerAddress =
        Address.extract(
            Bytes32.wrap(Hash.hash(committerNodeKey.getPublicKey().getEncodedBytes()).getBytes()));

    final List<Address> validators = singletonList(committerAddress);
    final BftContext bftContext = setupContextWithValidators(validators);
    final ProtocolContext context =
        new ProtocolContext.Builder().withConsensusContext(bftContext).build();
    stubCommitters(bftContext, emptyList());

    assertThat(commitSealsValidationRule.validate(blockHeader, null, context)).isFalse();
  }

  @Test
  public void committerNotInValidatorListFailsValidation() {
    final NodeKey committerNodeKey = NodeKeyUtils.generate();
    final Address committerAddress = Util.publicKeyToAddress(committerNodeKey.getPublicKey());

    final List<Address> validators = singletonList(committerAddress);

    // Insert an extraData block with committer seals.
    final NodeKey nonValidatorNodeKey = NodeKeyUtils.generate();

    final BftContext bftContext = setupContextWithValidators(validators);
    final ProtocolContext context =
        new ProtocolContext.Builder().withConsensusContext(bftContext).build();
    stubCommitters(
        bftContext, singletonList(Util.publicKeyToAddress(nonValidatorNodeKey.getPublicKey())));

    assertThat(commitSealsValidationRule.validate(blockHeader, null, context)).isFalse();
  }

  @Test
  public void ratioOfCommittersToValidatorsAffectValidation() {
    assertThat(subExecution(4, 4)).isEqualTo(true);
    assertThat(subExecution(4, 3)).isEqualTo(true);
    assertThat(subExecution(4, 2)).isEqualTo(false);

    assertThat(subExecution(5, 4)).isEqualTo(true);
    assertThat(subExecution(5, 3)).isEqualTo(false);
    assertThat(subExecution(5, 2)).isEqualTo(false);

    assertThat(subExecution(6, 4)).isEqualTo(true);
    assertThat(subExecution(6, 3)).isEqualTo(false);
    assertThat(subExecution(6, 2)).isEqualTo(false);

    assertThat(subExecution(7, 5)).isEqualTo(true);
    assertThat(subExecution(7, 4)).isEqualTo(false);

    assertThat(subExecution(8, 6)).isEqualTo(true);
    assertThat(subExecution(8, 5)).isEqualTo(false);
    assertThat(subExecution(8, 4)).isEqualTo(false);

    assertThat(subExecution(9, 6)).isEqualTo(true);
    assertThat(subExecution(9, 5)).isEqualTo(false);
    assertThat(subExecution(9, 4)).isEqualTo(false);

    assertThat(subExecution(10, 7)).isEqualTo(true);
    assertThat(subExecution(10, 6)).isEqualTo(false);

    assertThat(subExecution(12, 8)).isEqualTo(true);
    assertThat(subExecution(12, 7)).isEqualTo(false);
    assertThat(subExecution(12, 6)).isEqualTo(false);
  }

  @Test
  public void headerContainsDuplicateSealsFailsValidation() {
    final Address committerAddress =
        Util.publicKeyToAddress(NodeKeyUtils.generate().getPublicKey());
    final Address otherValidator = Util.publicKeyToAddress(NodeKeyUtils.generate().getPublicKey());
    // Two validators, so a two-seal header clears the seal-count bound and this test still
    // exercises the duplicate-committer branch rather than being rejected before it.
    final List<Address> validators = List.of(committerAddress, otherValidator);

    final BftContext bftContext = setupContextWithValidators(validators);
    final ProtocolContext context =
        new ProtocolContext.Builder().withConsensusContext(bftContext).build();
    stubCommitters(bftContext, List.of(committerAddress, committerAddress));

    assertThat(commitSealsValidationRule.validate(blockHeader, null, context)).isFalse();
  }

  @Test
  public void moreSealsThanValidatorsFailsValidationWithoutRecoveringAnyKey() {
    final List<Address> validators =
        List.of(
            Util.publicKeyToAddress(NodeKeyUtils.generate().getPublicKey()),
            Util.publicKeyToAddress(NodeKeyUtils.generate().getPublicKey()));

    final BftContext bftContext = setupContextWithValidators(validators);
    final ProtocolContext context =
        new ProtocolContext.Builder().withConsensusContext(bftContext).build();

    // 50,000 seals against a 2-validator set. The header must be rejected on the count alone.
    final BftExtraData extraData = mock(BftExtraData.class);
    when(extraData.getSeals()).thenReturn(Collections.nCopies(50_000, null));
    when(bftContext.getBlockInterface().getExtraData(any())).thenReturn(extraData);

    assertThat(commitSealsValidationRule.validate(blockHeader, null, context)).isFalse();

    // This is the finding: no ecrecover may run for a header rejectable on an integer comparison.
    verify(bftContext.getBlockInterface(), never()).getCommitters(any());
  }

  /**
   * Stubs the committers the rule will recover, and the raw seal list it now counts before
   * recovering anything. Real headers carry one seal per committer, so the two stay in step.
   */
  private static void stubCommitters(final BftContext bftContext, final List<Address> committers) {
    final BftExtraData extraData = mock(BftExtraData.class);
    when(extraData.getSeals()).thenReturn(Collections.nCopies(committers.size(), null));
    when(bftContext.getBlockInterface().getExtraData(any())).thenReturn(extraData);
    when(bftContext.getBlockInterface().getCommitters(any())).thenReturn(committers);
  }

  private boolean subExecution(final int validatorCount, final int committerCount) {

    final List<Address> validators = Lists.newArrayList();

    for (int i = 0; i < validatorCount; i++) { // need -1 to account for proposer
      final NodeKey committerNodeKey = NodeKeyUtils.generate();
      validators.add(
          Address.extract(
              Bytes32.wrap(
                  Hash.hash(committerNodeKey.getPublicKey().getEncodedBytes()).getBytes())));
    }

    Collections.sort(validators);

    final BftContext bftContext = setupContextWithValidators(validators);
    final ProtocolContext context =
        new ProtocolContext.Builder().withConsensusContext(bftContext).build();
    stubCommitters(bftContext, validators.subList(0, committerCount));

    return commitSealsValidationRule.validate(blockHeader, null, context);
  }
}
