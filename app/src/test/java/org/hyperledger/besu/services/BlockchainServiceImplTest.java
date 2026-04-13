/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link BlockchainServiceImpl} throws a clear {@link IllegalStateException} when its
 * query methods are invoked before {@link BlockchainServiceImpl#init} has been called, rather than
 * a confusing {@link NullPointerException}.
 *
 * <p>This guards against the "hollow service" lifecycle pitfall: {@code BlockchainService} is
 * registered in the plugin context during the REGISTERING phase (so plugins can obtain a reference)
 * but is only fully initialised in the STARTED phase once the {@code BesuController} is ready.
 */
class BlockchainServiceImplTest {

  private BlockchainServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new BlockchainServiceImpl();
  }

  @Test
  void getBlockByNumber_beforeInit_throwsIllegalStateException() {
    assertThatThrownBy(() -> service.getBlockByNumber(1L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("BlockchainService is not yet fully initialized")
        .hasMessageContaining("start()");
  }

  @Test
  void getBlockByHash_beforeInit_throwsIllegalStateException() {
    assertThatThrownBy(() -> service.getBlockByHash(Hash.ZERO))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("BlockchainService is not yet fully initialized");
  }

  @Test
  void getBlockHeaderByHash_beforeInit_throwsIllegalStateException() {
    assertThatThrownBy(() -> service.getBlockHeaderByHash(Hash.ZERO))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("BlockchainService is not yet fully initialized");
  }

  @Test
  void getChainHeadHash_beforeInit_throwsIllegalStateException() {
    assertThatThrownBy(() -> service.getChainHeadHash())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("BlockchainService is not yet fully initialized");
  }

  @Test
  void getChainHeadHeader_beforeInit_throwsIllegalStateException() {
    assertThatThrownBy(() -> service.getChainHeadHeader())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("BlockchainService is not yet fully initialized");
  }

  @Test
  void queryMethods_afterInit_doNotThrow() {
    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);

    org.mockito.Mockito.when(blockchain.getBlockByNumber(1L))
        .thenReturn(java.util.Optional.empty());
    org.mockito.Mockito.when(blockchain.getBlockByHash(Hash.ZERO))
        .thenReturn(java.util.Optional.empty());

    service.init(blockchain, protocolSchedule);

    // After init, calls should delegate to the blockchain mock without IllegalStateException
    assertThatCode(() -> service.getBlockByNumber(1L)).doesNotThrowAnyException();
    assertThatCode(() -> service.getBlockByHash(Hash.ZERO)).doesNotThrowAnyException();
  }
}
