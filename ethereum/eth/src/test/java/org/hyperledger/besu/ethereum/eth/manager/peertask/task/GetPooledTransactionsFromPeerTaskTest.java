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
package org.hyperledger.besu.ethereum.eth.manager.peertask.task;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.PooledTransactionsMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GetPooledTransactionsFromPeerTaskTest {
  private static final BlockDataGenerator GENERATOR = new BlockDataGenerator();
  private static final Set<Capability> AGREED_CAPABILITIES = Set.of(EthProtocol.LATEST);

  @Test
  public void testGetRequestMessage() {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    MessageData result = task.getRequestMessage(AGREED_CAPABILITIES);

    Assertions.assertEquals(
        "0xe1a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
        result.getData().toHexString());
  }

  @Test
  public void testProcessResponse() throws InvalidPeerTaskResponseException {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    Transaction transaction = GENERATOR.transaction();
    PooledTransactionsMessage pooledTransactionsMessage =
        PooledTransactionsMessage.create(List.of(transaction));

    List<Transaction> result = task.processResponse(pooledTransactionsMessage, AGREED_CAPABILITIES);

    Assertions.assertEquals(List.of(transaction), result);
  }

  @Test
  public void testProcessResponseWithIncorrectTransactionCount() {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    PooledTransactionsMessage pooledTransactionsMessage =
        PooledTransactionsMessage.create(List.of(GENERATOR.transaction(), GENERATOR.transaction()));

    InvalidPeerTaskResponseException exception =
        Assertions.assertThrows(
            InvalidPeerTaskResponseException.class,
            () -> task.processResponse(pooledTransactionsMessage, AGREED_CAPABILITIES));

    Assertions.assertEquals(
        "Response transaction count does not match request hash count", exception.getMessage());
  }

  @Test
  public void testValidateResult() {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    Transaction transaction = Mockito.mock(Transaction.class);
    Mockito.when(transaction.getHash()).thenReturn(Hash.EMPTY);

    PeerTaskValidationResponse validationResponse = task.validateResult(List.of(transaction));
    Assertions.assertEquals(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD, validationResponse);
  }

  @Test
  public void testValidateResultWithMismatchedResults() {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    Transaction transaction = Mockito.mock(Transaction.class);
    Mockito.when(transaction.getHash()).thenReturn(Hash.EMPTY_TRIE_HASH);

    PeerTaskValidationResponse validationResponse = task.validateResult(List.of(transaction));
    Assertions.assertEquals(
        PeerTaskValidationResponse.RESULTS_DO_NOT_MATCH_QUERY, validationResponse);
  }
}
