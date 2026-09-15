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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.evm.account.Account;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the parts of witness assembly the execution-spec fixtures cannot reach:
 *
 * <ul>
 *   <li>the ancestry walk in {@code buildHeaders} — fixture chains are linear, so
 *       canonical-by-height and true ancestry agree and a regression would pass unnoticed;
 *   <li>{@code buildCodes} filtering, which decides what the block access list contributes;
 *   <li>the failure paths, which fixtures never exercise because they always supply a trie log, a
 *       parent world state and a block access list.
 * </ul>
 */
class BonsaiExecutionWitnessBuilderTest {

  private final Blockchain blockchain = mock(Blockchain.class);
  private final PathBasedWorldStateProvider worldStateProvider =
      mock(PathBasedWorldStateProvider.class);
  private final BonsaiExecutionWitnessBuilder builder =
      new BonsaiExecutionWitnessBuilder(worldStateProvider, blockchain);

  private static final Address ADDR_A =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address ADDR_B =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");
  private static final Address ADDR_C =
      Address.fromHexString("0x00000000000000000000000000000000000000cc");

  /** A block access list that reports the given addresses as touched and nothing else. */
  private static BlockAccessList balTouching(final Address... addresses) {
    final List<AccountChanges> changes = new ArrayList<>();
    for (final Address a : addresses) {
      changes.add(new AccountChanges(a, List.of(), List.of(), List.of(), List.of(), List.of()));
    }
    return new BlockAccessList(changes);
  }

  /** Stubs an account whose code is {@code code}; pass null for an account absent from state. */
  private static void stubAccount(
      final BonsaiWorldState worldView, final Address address, final Bytes code) {
    if (code == null) {
      when(worldView.get(address)).thenReturn(null);
      return;
    }
    final Account account = mock(Account.class);
    final Hash codeHash = code.isEmpty() ? Hash.EMPTY : Hash.hash(code);
    when(account.getCodeHash()).thenReturn(codeHash);
    when(worldView.get(address)).thenReturn(account);
    when(worldView.getCode(address, codeHash)).thenReturn(Optional.of(code));
  }

  /** Chains headers 0..count-1, each pointing at the previous, tagged so forks differ by hash. */
  private List<BlockHeader> chain(final int count, final long extraData) {
    final List<BlockHeader> headers = new ArrayList<>();
    Hash parent = Hash.ZERO;
    for (int i = 0; i < count; i++) {
      final BlockHeader h =
          new BlockHeaderTestFixture()
              .number(i)
              .parentHash(parent)
              .gasLimit(30_000_000L + extraData)
              .buildHeader();
      headers.add(h);
      parent = h.getHash();
    }
    return headers;
  }

  private void registerByHash(final List<BlockHeader> headers) {
    headers.forEach(h -> when(blockchain.getBlockHeader(h.getHash())).thenReturn(Optional.of(h)));
  }

  private static String rlp(final BlockHeader header) {
    return RLP.encode(header::writeTo).toHexString();
  }

  @Test
  void shouldIncludePreStateCodeOfEveryTouchedAccount() {
    final BonsaiWorldState worldView = mock(BonsaiWorldState.class);
    stubAccount(worldView, ADDR_A, Bytes.fromHexString("0x6001"));
    stubAccount(worldView, ADDR_B, Bytes.fromHexString("0x6002"));

    assertThat(builder.buildCodes(worldView, balTouching(ADDR_A, ADDR_B)))
        .containsExactly("0x6001", "0x6002");
  }

  @Test
  void shouldSkipAccountsWithEmptyCode() {
    // The common case: the block access list lists every touched account, most of them EOAs.
    final BonsaiWorldState worldView = mock(BonsaiWorldState.class);
    stubAccount(worldView, ADDR_A, Bytes.EMPTY);
    stubAccount(worldView, ADDR_B, Bytes.fromHexString("0x6002"));

    assertThat(builder.buildCodes(worldView, balTouching(ADDR_A, ADDR_B)))
        .containsExactly("0x6002");
  }

  @Test
  void shouldSkipAccountsAbsentFromParentState() {
    // An account created during the block is touched but has no pre-state entry to contribute.
    final BonsaiWorldState worldView = mock(BonsaiWorldState.class);
    stubAccount(worldView, ADDR_A, null);
    stubAccount(worldView, ADDR_B, Bytes.fromHexString("0x6002"));

    assertThat(builder.buildCodes(worldView, balTouching(ADDR_A, ADDR_B)))
        .containsExactly("0x6002");
  }

  @Test
  void shouldDeduplicateCodeSharedBySeveralAddresses() {
    // Codes is a list of bytecodes, not a map, so two proxies with identical code contribute once.
    final BonsaiWorldState worldView = mock(BonsaiWorldState.class);
    final Bytes shared = Bytes.fromHexString("0x600160025500");
    stubAccount(worldView, ADDR_A, shared);
    stubAccount(worldView, ADDR_B, shared);

    assertThat(builder.buildCodes(worldView, balTouching(ADDR_A, ADDR_B)))
        .containsExactly(shared.toHexString());
  }

  @Test
  void shouldSortCodesRegardlessOfBlockAccessListOrder() {
    final BonsaiWorldState worldView = mock(BonsaiWorldState.class);
    stubAccount(worldView, ADDR_A, Bytes.fromHexString("0x60ff"));
    stubAccount(worldView, ADDR_B, Bytes.fromHexString("0x6001"));
    stubAccount(worldView, ADDR_C, Bytes.fromHexString("0x6080"));

    assertThat(builder.buildCodes(worldView, balTouching(ADDR_C, ADDR_A, ADDR_B)))
        .containsExactly("0x6001", "0x6080", "0x60ff");
  }

  @Test
  void shouldReturnNoCodesWhenBlockAccessListIsEmpty() {
    // A pre-Amsterdam block produces no block access list entries, so there is nothing to derive
    // codes from. The RPC rejects such a block before reaching the builder; this pins the builder's
    // own behaviour so it degrades to empty rather than throwing.
    final BonsaiWorldState worldView = mock(BonsaiWorldState.class);

    assertThat(builder.buildCodes(worldView, balTouching())).isEmpty();
  }

  @Test
  void shouldThrowWhenTrieLogIsMissing() {
    final List<BlockHeader> headers = chain(3, 0);
    registerByHash(headers);
    final BlockHeader block = headers.get(2);

    final TrieLogManager trieLogManager = mock(TrieLogManager.class);
    when(worldStateProvider.getTrieLogManager()).thenReturn(trieLogManager);
    when(trieLogManager.getTrieLogLayer(block.getHash())).thenReturn(Optional.empty());

    assertThatThrownBy(() -> builder.buildWitness(block, balTouching(), Map.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("trie log missing")
        .hasMessageContaining(block.getHash().toString());
  }

  @Test
  void shouldThrowWhenParentWorldStateUnavailable() {
    final List<BlockHeader> headers = chain(3, 0);
    registerByHash(headers);
    final BlockHeader block = headers.get(2);
    final BlockHeader parent = headers.get(1);

    final TrieLogManager trieLogManager = mock(TrieLogManager.class);
    when(worldStateProvider.getTrieLogManager()).thenReturn(trieLogManager);
    when(trieLogManager.getTrieLogLayer(block.getHash()))
        .thenReturn(Optional.of(mock(org.hyperledger.besu.plugin.services.trielogs.TrieLog.class)));
    when(worldStateProvider.getWorldState(any())).thenReturn(Optional.empty());

    assertThatThrownBy(() -> builder.buildWitness(block, balTouching(), Map.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("parent world state unavailable")
        .hasMessageContaining(parent.getHash().toString());
  }

  @Test
  void shouldThrowWhenWorldStateArchiveIsNotPathBased() {
    // Forest nodes cannot produce a witness; the builder is created per request so this surfaces as
    // a per-request error rather than a startup failure.
    assertThatThrownBy(
            () ->
                new BonsaiExecutionWitnessBuilder(
                    mock(org.hyperledger.besu.ethereum.worldstate.WorldStateArchive.class),
                    blockchain))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PathBasedWorldStateProvider");
  }

  @Test
  void shouldFollowBlockOwnAncestryRatherThanCanonicalChain() {
    // Canonical chain 0..4, and a fork that diverges after block 2 with different hashes at 3 and
    // 4.
    final List<BlockHeader> canonical = chain(5, 0);
    final List<BlockHeader> fork = new ArrayList<>(canonical.subList(0, 3));
    for (int i = 3; i < 5; i++) {
      fork.add(
          new BlockHeaderTestFixture()
              .number(i)
              .parentHash(fork.get(i - 1).getHash())
              .gasLimit(31_000_000L) // differs from canonical, so the hash differs too
              .buildHeader());
    }
    registerByHash(canonical);
    registerByHash(fork);
    // Height lookups resolve on the canonical chain - what the buggy implementation used.
    canonical.forEach(
        h -> when(blockchain.getBlockHeader(h.getNumber())).thenReturn(Optional.of(h)));

    assertThat(fork.get(3).getHash()).isNotEqualTo(canonical.get(3).getHash());

    // Witness for the fork's block 4: ancestors 1..3 must come from the fork, not the canonical
    // chain.
    final List<String> headers = builder.buildHeaders(1L, fork.get(4));

    assertThat(headers)
        .as("ancestors must be the block's own, ascending by number")
        .containsExactly(rlp(fork.get(1)), rlp(fork.get(2)), rlp(fork.get(3)));
    assertThat(headers).doesNotContain(rlp(canonical.get(3)));
    verify(blockchain, never()).getBlockHeader(anyLong());
  }

  @Test
  void shouldReturnHeadersAscendingFromOldestAncestor() {
    final List<BlockHeader> headers = chain(6, 0);
    registerByHash(headers);

    final List<String> result = builder.buildHeaders(2L, headers.get(5));

    assertThat(result)
        .containsExactly(rlp(headers.get(2)), rlp(headers.get(3)), rlp(headers.get(4)));
  }

  @Test
  void shouldStopAtGenesisWhenOldestAncestorIsBelowIt() {
    final List<BlockHeader> headers = chain(3, 0);
    registerByHash(headers);

    // Genuinely below genesis: -1 would otherwise resolve the zero parent hash and throw.
    final List<String> result = builder.buildHeaders(-1L, headers.get(2));

    assertThat(result).containsExactly(rlp(headers.get(0)), rlp(headers.get(1)));
  }

  @Test
  void shouldReturnSingleHeaderWhenOnlyParentIsAccessed() {
    final List<BlockHeader> headers = chain(4, 0);
    registerByHash(headers);

    final List<String> result = builder.buildHeaders(2L, headers.get(3));

    assertThat(result).containsExactly(rlp(headers.get(2)));
  }
}
