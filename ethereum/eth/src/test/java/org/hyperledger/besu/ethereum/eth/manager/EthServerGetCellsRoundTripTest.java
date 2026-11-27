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
package org.hyperledger.besu.ethereum.eth.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper.CELLS_PER_EXT_BLOB;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.CellsOnlyBlobTransactionFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetCellsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.messages.GetCellsMessage;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Both ends of the cells exchange in one piece: what {@code EthServer} serves from a real sidecar
 * must come back out of {@code GetCellsFromPeerTask} as cells that open their commitments. The two
 * sides share the blob-major ordering only as a convention; this is where a disagreement would
 * surface, as a KZG verification failure.
 */
class EthServerGetCellsRoundTripTest extends TrustedSetupClassLoaderExtension {

  @Test
  void theTaskAcceptsWhatTheServerServes() throws Exception {
    final BlobsWithCommitments sidecar =
        CKZG4844Helper.convertToVersion1(new BlobTestFixture().createBlobsWithCommitments(2));
    final Transaction tx = new CellsOnlyBlobTransactionFixture().create(sidecar);

    final TransactionPool transactionPool = mock(TransactionPool.class);
    when(transactionPool.getTransactionByHash(tx.getHash())).thenReturn(Optional.of(tx));

    // a partial request, so selecting a subset in blob-major order is what is round-tripped
    final CellMask requested = range(0, 64);
    final MessageData response =
        EthServer.constructGetCellsResponse(
            transactionPool,
            mock(EthPeer.class),
            GetCellsMessage.create(List.of(tx), requested),
            16,
            EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE);

    final GetCellsFromPeerTask task = new GetCellsFromPeerTask(tx, requested);
    final List<CellsWithMask> result = task.processResponse(response, Set.of());

    assertThat(result).hasSize(2);
    assertThat(result).allSatisfy(group -> assertThat(group.getCellMask()).isEqualTo(requested));
    assertThat(task.validateResult(result))
        .isEqualTo(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);
  }

  private static CellMask range(final int fromInclusive, final int toExclusive) {
    final BitSet bits = new BitSet(CELLS_PER_EXT_BLOB);
    bits.set(fromInclusive, toExclusive);
    final byte[] bytes = new byte[CellMask.BYTE_LENGTH];
    final byte[] set = bits.toByteArray();
    System.arraycopy(set, 0, bytes, 0, set.length);
    return CellMask.fromBytes(Bytes.wrap(bytes));
  }
}
