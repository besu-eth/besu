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
package org.hyperledger.besu.consensus.qbft.sync;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.qbft.BFTPivotSelectorFromPeers;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.NoSyncRequiredException;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class QbftPivotSelectorTest {
  private final SynchronizerConfiguration.Builder syncConfigBuilder =
      new SynchronizerConfiguration.Builder().syncMode(SyncMode.SNAP).syncMinimumPeerCount(1);
  private final SynchronizerConfiguration syncConfig = syncConfigBuilder.build();
  @Mock private NodeKey nodeKey;
  @Mock private BlockHeader blockHeader;
  @Mock private ProtocolContext protocolContext;
  @Mock private BftContext bftContext;
  @Mock private SyncState syncState;
  @Mock private EthContext ethContext;
  @Mock private EthPeers ethPeers;
  @Mock private ValidatorProvider validatorProvider;

  @BeforeEach
  public void setUp() {
    when(protocolContext.getConsensusContext(any())).thenReturn(bftContext);
    when(bftContext.getValidatorProvider()).thenReturn(validatorProvider);
  }

  @Test
  public void returnEmptySyncStateIfValidatorWithOtherValidatorsButNoPeers() {

    // We're not the only validator so add some other ones
    List<Address> validatorList = new ArrayList<>();
    validatorList.add(Address.ZERO);
    validatorList.add(Address.ZERO);
    validatorList.add(Address.ZERO);

    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    when(validatorProvider.nodeIsValidator(any())).thenReturn(true);
    when(validatorProvider.getValidatorsAtHead()).thenReturn(validatorList);
    BFTPivotSelectorFromPeers pivotSelector =
        new BFTPivotSelectorFromPeers(
            ethContext, syncConfig, syncState, protocolContext, nodeKey, blockHeader);

    assertThatThrownBy(() -> pivotSelector.selectNewPivotBlock().get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("Not enough peers");
  }

  @Test
  @SuppressWarnings("unused")
  public void returnNoSyncRequiredIfOnlyValidatorAndNoPeers() {

    // We're the only validator so just put us in the list
    List<Address> validatorList = new ArrayList<>();
    validatorList.add(Address.ZERO);

    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    when(validatorProvider.nodeIsValidator(any())).thenReturn(true);
    when(validatorProvider.getValidatorsAtHead()).thenReturn(validatorList);
    BFTPivotSelectorFromPeers pivotSelector =
        new BFTPivotSelectorFromPeers(
            ethContext, syncConfig, syncState, protocolContext, nodeKey, blockHeader);

    try {
      pivotSelector.selectNewPivotBlock();
      fail("Expected NoSyncRequiredException but none thrown");
    } catch (NoSyncRequiredException e) {
      // Ignore - just make sure we get here
    }
  }

  @Test
  public void returnEmptySyncStateIfNonValidatorWithNoBestPeer() {
    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    when(validatorProvider.nodeIsValidator(any())).thenReturn(false);
    BFTPivotSelectorFromPeers pivotSelector =
        new BFTPivotSelectorFromPeers(
            ethContext, syncConfig, syncState, protocolContext, nodeKey, blockHeader);

    assertThatThrownBy(() -> pivotSelector.selectNewPivotBlock().get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("Not enough peers");
  }

  @Test
  public void returnEmptySyncStateIfValidatorAndNotAtGenesisAndOtherValidators() {
    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    when(validatorProvider.nodeIsValidator(any())).thenReturn(false);
    when(blockHeader.getNumber()).thenReturn(10L);
    BFTPivotSelectorFromPeers pivotSelector =
        new BFTPivotSelectorFromPeers(
            ethContext, syncConfig, syncState, protocolContext, nodeKey, blockHeader);

    assertThatThrownBy(() -> pivotSelector.selectNewPivotBlock().get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("Not enough peers");
  }
}
