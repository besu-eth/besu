/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetStorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Serves snap protocol account ranges, storage ranges and bytecodes from a real in-memory world
 * state through the real {@link SnapServer} response constructors (real proof generation), for
 * tests that need a peer-like data source without any peer machinery.
 */
public class SnapTestServing {

  private static final BigInteger NO_SIZE_LIMIT = BigInteger.valueOf(Integer.MAX_VALUE);

  private final SnapServer snapServer;
  private final Hash servedStateRoot;

  /**
   * @param storage the world state to serve from (flat db and tries must be populated)
   * @param servedStateRoot the state root the server answers for; requests for any other root get
   *     empty responses
   */
  public SnapTestServing(
      final BonsaiWorldStateKeyValueStorage storage, final Hash servedStateRoot) {
    this.servedStateRoot = servedStateRoot;
    this.snapServer =
        new SnapServer(
                new EthMessages(),
                new WorldStateStorageCoordinator(storage),
                rootHash ->
                    servedStateRoot.equals(rootHash) ? Optional.of(storage) : Optional.empty(),
                Long.MAX_VALUE)
            .start();
  }

  public CompletableFuture<AccountRangeMessage.AccountRangeData> accountRange(
      final Bytes32 startKeyHash, final Bytes32 endKeyHash, final BlockHeader pivotBlockHeader) {
    final MessageData response =
        snapServer.constructGetAccountRangeResponse(
            GetAccountRangeMessage.create(servedStateRoot, startKeyHash, endKeyHash, NO_SIZE_LIMIT)
                .wrapMessageData(BigInteger.ONE));
    return CompletableFuture.completedFuture(
        AccountRangeMessage.readFrom(response).accountData(false));
  }

  public CompletableFuture<StorageRangeMessage.SlotRangeData> storageRange(
      final List<Bytes32> accountHashes,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash,
      final BlockHeader pivotBlockHeader) {
    final MessageData response =
        snapServer.constructGetStorageRangeResponse(
            GetStorageRangeMessage.create(servedStateRoot, accountHashes, startKeyHash, endKeyHash)
                .wrapMessageData(BigInteger.ONE));
    return CompletableFuture.completedFuture(
        StorageRangeMessage.readFrom(response).slotsData(false));
  }

  public CompletableFuture<Map<Bytes32, Bytes>> byteCodes(
      final List<Bytes32> codeHashes, final BlockHeader pivotBlockHeader) {
    final MessageData response =
        snapServer.constructGetBytecodesResponse(
            GetByteCodesMessage.create(codeHashes).wrapMessageData(BigInteger.ONE));
    final Map<Bytes32, Bytes> codeByHash = new HashMap<>();
    for (final Bytes code : ByteCodesMessage.readFrom(response).bytecodes(false).codes()) {
      codeByHash.put(Bytes32.wrap(Hash.hash(code).getBytes()), code);
    }
    return CompletableFuture.completedFuture(codeByHash);
  }
}
