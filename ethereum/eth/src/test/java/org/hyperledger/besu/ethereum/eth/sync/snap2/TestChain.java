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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes32;

/**
 * A chain of empty proof-of-stake blocks whose state changes through their block access lists. The
 * network answers at once for its headers, bodies and lists, and serves the state of each block.
 */
final class TestChain implements ChainHeadSource {

  private static final BlockBody EMPTY_BODY =
      new BlockBody(List.of(), List.of(), Optional.of(List.of()));
  // accounts and slots a block changes
  private static final int CHANGES_PER_BLOCK = 4;

  private final Random random;
  private final FakeSnapNetwork network;
  private final MutableBlockchain source;
  private final ChainSender sender;
  // the world of each block of the chain, by number
  private final List<TestWorld> worlds = new ArrayList<>();

  /** Creates a chain of its genesis only. */
  TestChain(final Random random, final TestWorld genesisWorld, final FakeSnapNetwork network) {
    this.random = random;
    this.network = network;
    final BlockHeader genesis =
        new BlockHeaderTestFixture()
            .number(0)
            .stateRoot(Hash.wrap(genesisWorld.root()))
            .buildHeader();
    source =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(new Block(genesis, EMPTY_BODY));
    sender = new ChainSender(source);
    worlds.add(genesisWorld);
    network.serve(genesisWorld);
    network.serveChain(sender);
  }

  /** Returns the genesis block, to create the blockchain of the node from. */
  Block genesisBlock() {
    return source.getGenesisBlock();
  }

  /** Returns what answers the requests of the chain, to fail some of them. */
  ChainSender sender() {
    return sender;
  }

  /** Appends blocks, the last one becoming the head. */
  void extend(final int blocks) {
    for (int i = 0; i < blocks; i++) {
      final TestWorld world = worlds.getLast().copy();
      final BlockAccessList accessList = world.mutate(random, CHANGES_PER_BLOCK);
      final BlockHeader header =
          header(source.getChainHeadHeader(), world.root(), BodyValidation.balHash(accessList));
      source.appendBlock(new Block(header, EMPTY_BODY), List.of(), Optional.of(accessList));
      worlds.add(world);
      network.serve(world);
    }
  }

  /** Replaces the blocks above {@code ancestor} with a fork of {@code blocks} new blocks. */
  void reorg(final long ancestor, final int blocks) {
    source.rewindToBlock(ancestor);
    while (worlds.size() > ancestor + 1) {
      worlds.removeLast();
    }
    extend(blocks);
  }

  BlockHeader header(final long number) {
    return source.getBlockHeader(number).orElseThrow();
  }

  /** Returns the headers from {@code from} to {@code to}, both included. */
  List<BlockHeader> headers(final long from, final long to) {
    final List<BlockHeader> headers = new ArrayList<>();
    for (long number = from; number <= to; number++) {
      headers.add(header(number));
    }
    return headers;
  }

  TestWorld world(final long number) {
    return worlds.get((int) number);
  }

  @Override
  public Optional<Hash> headHash() {
    return Optional.of(source.getChainHeadHash());
  }

  // as the consensus client sends every block with a new payload
  @Override
  public Optional<BlockHeader> knownHeader(final Hash hash) {
    return source.getBlockHeader(hash);
  }

  private static BlockHeader header(
      final BlockHeader parent, final Bytes32 stateRoot, final Hash balHash) {
    final long number = parent.getNumber() + 1;
    return new BlockHeaderTestFixture()
        .number(number)
        .parentHash(parent.getHash())
        .stateRoot(Hash.wrap(stateRoot))
        .timestamp(number * 12)
        .gasLimit(30_000_000)
        .baseFeePerGas(Wei.of(7))
        .withdrawalsRoot(Hash.EMPTY_TRIE_HASH)
        .blobGasUsed(0L)
        .excessBlobGas(BlobGas.ZERO)
        .parentBeaconBlockRoot(Optional.of(Bytes32.ZERO))
        .requestsHash(Hash.EMPTY_REQUESTS_HASH)
        .balHash(balHash)
        .buildHeader();
  }
}
