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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.BlockchainImporter;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcTestMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.testutil.BlockTestUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.io.Resources;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DebugGetModifiedAccountsIntegrationTest {

  private static final String BY_NUMBER = "debug_getModifiedAccountsByNumber";
  private static final String BY_HASH = "debug_getModifiedAccountsByHash";

  private static MutableBlockchain blockchain;
  private static BlockchainQueries blockchainQueries;
  private static Map<String, JsonRpcMethod> methods;
  private static long chainHead;

  @BeforeAll
  public static void setUpOnce() throws Exception {
    final String genesisJson =
        Resources.toString(BlockTestUtil.getTestGenesisUrl(), StandardCharsets.UTF_8);
    final BlockchainImporter importer =
        new BlockchainImporter(BlockTestUtil.getTestBlockchainUrl(), genesisJson);

    blockchain =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(importer.getGenesisBlock());
    final BonsaiWorldStateProvider worldStateArchive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
    importer.getGenesisState().writeStateTo(worldStateArchive.getWorldState());
    final ProtocolContext context =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(worldStateArchive)
            .build();

    for (final Block block : importer.getBlocks()) {
      final ProtocolSpec protocolSpec =
          importer.getProtocolSchedule().getByBlockHeader(block.getHeader());
      protocolSpec.getBlockImporter().importBlock(context, block, HeaderValidationMode.FULL);
    }

    final JsonRpcTestMethodsFactory factory =
        new JsonRpcTestMethodsFactory(importer, blockchain, worldStateArchive, context);
    methods = factory.methods();
    blockchainQueries = factory.getBlockchainQueries();
    chainHead = blockchain.getChainHeadBlockNumber();
    assertThat(chainHead).isGreaterThan(1L);
  }

  @Test
  public void bothMethodsAreRegisteredOnABonsaiNode() {
    assertThat(methods).containsKeys(BY_NUMBER, BY_HASH);
  }

  @Test
  public void everyBlockReportsTheAccountsItChanged() {
    for (long block = 1; block <= chainHead; block++) {
      final long number = block;
      final Block currentBlock = block(number);
      final BlockHeader header = currentBlock.getHeader();
      final List<String> modified = result(BY_NUMBER, hex(number));

      assertThat(modified).describedAs("block %d", number).isSorted().doesNotHaveDuplicates();
      assertThat(modified)
          .describedAs("coinbase of block %d", number)
          .contains(header.getCoinbase().toString());
      for (final Transaction transaction : currentBlock.getBody().getTransactions()) {
        assertThat(modified)
            .describedAs("sender of %s in block %d", transaction.getHash(), number)
            .contains(transaction.getSender().toString());
        transaction
            .getTo()
            .ifPresent(
                to ->
                    assertThat(modified)
                        .describedAs("recipient of %s in block %d", transaction.getHash(), number)
                        .contains(to.toString()));
      }
    }
  }

  @Test
  public void reportedAccountsReallyDifferAtTheEndsOfTheDiff() {
    final BlockHeader withTransactions = firstBlockWithTransactions().getHeader();
    assertEveryReportedAccountChanged(
        withTransactions.getParentHash(),
        withTransactions.getBlockHash(),
        result(BY_NUMBER, hex(withTransactions.getNumber())));

    final BlockHeader head = header(chainHead);
    assertEveryReportedAccountChanged(
        head.getParentHash(), head.getBlockHash(), result(BY_NUMBER, hex(chainHead)));

    assertEveryReportedAccountChanged(
        header(0).getBlockHash(), head.getBlockHash(), result(BY_NUMBER, hex(0), hex(chainHead)));
  }

  @Test
  public void theParamFormsAgreeForEveryBlock() {
    for (long number = 1; number <= chainHead; number++) {
      final String blockHash = header(number).getBlockHash().toString();
      final String parentHash = header(number - 1).getBlockHash().toString();
      final List<String> singleBlock = result(BY_NUMBER, hex(number));

      assertThat(result(BY_NUMBER, hex(number - 1), hex(number)))
          .describedAs("explicit parent range of block %d", number)
          .isEqualTo(singleBlock);
      assertThat(result(BY_HASH, blockHash))
          .describedAs("by hash, block %d", number)
          .isEqualTo(singleBlock);
      assertThat(result(BY_HASH, parentHash, blockHash))
          .describedAs("by hash range ending at block %d", number)
          .isEqualTo(singleBlock);
    }
  }

  @Test
  public void rangeIsTheAccumulationOfTheBlocksItSpans() {
    final List<String> range = result(BY_NUMBER, hex(0), hex(chainHead));

    assertThat(range).isNotEmpty().isSorted().doesNotHaveDuplicates();
    assertThat(range).isSubsetOf(everyBlockUpTo(chainHead));
    assertThat(range).containsAll(sendersUpTo(chainHead));
  }

  @Test
  public void accountsDroppedFromTheRangeAreUnchangedEndToEnd() {
    final Hash start = header(0).getBlockHash();
    final Hash end = header(chainHead).getBlockHash();
    final Set<String> dropped = everyBlockUpTo(chainHead);
    dropped.removeAll(result(BY_NUMBER, hex(0), hex(chainHead)));

    for (final String address : dropped) {
      final Address account = Address.fromHexString(address);
      assertThat(accountState(end, account))
          .describedAs("state of %s, reported by a block but not by the range", address)
          .isEqualTo(accountState(start, account));
    }
  }

  @Test
  public void contractCreationIsReported() {
    final Block block = firstBlockWithContractCreation();
    final Transaction creation =
        block.getBody().getTransactions().stream()
            .filter(Transaction::isContractCreation)
            .findFirst()
            .orElseThrow();
    final Address created = Address.contractAddress(creation.getSender(), creation.getNonce());
    final BlockHeader header = block.getHeader();

    assertThat(result(BY_NUMBER, hex(header.getNumber()))).contains(created.toString());
    assertThat(accountState(header.getParentHash(), created)).isEmpty();
    assertThat(codeHash(header.getBlockHash(), created)).isPresent().get().isNotEqualTo(Hash.EMPTY);
  }

  @Test
  public void genesisHasNoParentToDiffAgainst() {
    assertThat(error(BY_NUMBER, hex(0))).isEqualTo("block 0 has no parent");
    assertThat(error(BY_HASH, header(0).getBlockHash().toString()))
        .isEqualTo("block 0 has no parent");
  }

  @Test
  public void unknownBlockIsAnError() {
    assertThat(error(BY_NUMBER, hex(chainHead + 1), hex(chainHead + 2)))
        .isEqualTo("start block " + Long.toHexString(chainHead + 1) + " not found");
    assertThat(error(BY_NUMBER, hex(1), hex(chainHead + 1)))
        .isEqualTo("end block " + (chainHead + 1) + " not found");
    assertThat(error(BY_HASH, Hash.ZERO.toString(), header(chainHead).getBlockHash().toString()))
        .isEqualTo(
            "start block 0000000000000000000000000000000000000000000000000000000000000000 not found");
  }

  @Test
  public void startBlockNotBeforeEndBlockIsAnError() {
    assertThat(error(BY_NUMBER, hex(chainHead), hex(1)))
        .isEqualTo(
            String.format(
                "start block height (%d) must be less than end block height (1)", chainHead));
    assertThat(error(BY_NUMBER, hex(1), hex(1)))
        .isEqualTo("start block height (1) must be less than end block height (1)");
  }

  @Test
  public void nonNumericBlockParameterIsRejected() {
    assertThatThrownBy(() -> response(BY_NUMBER, "latest"))
        .isInstanceOf(InvalidJsonRpcParameters.class);
    assertThatThrownBy(() -> response(BY_NUMBER, hex(1), "pending"))
        .isInstanceOf(InvalidJsonRpcParameters.class);
    assertThatThrownBy(() -> response(BY_HASH, "not-a-hash"))
        .isInstanceOf(InvalidJsonRpcParameters.class);
  }

  private void assertEveryReportedAccountChanged(
      final Hash startBlockHash, final Hash endBlockHash, final List<String> reported) {
    assertThat(reported).isNotEmpty();
    for (final String address : reported) {
      final Address account = Address.fromHexString(address);
      assertThat(accountState(endBlockHash, account))
          .describedAs("state of %s at the end of the diff", address)
          .isNotEqualTo(accountState(startBlockHash, account));
    }
  }

  private Optional<String> accountState(final Hash blockHash, final Address address) {
    return account(
        blockHash,
        address,
        account ->
            String.join(
                ":",
                Long.toString(account.getNonce()),
                account.getBalance().toString(),
                account.getCodeHash().toString(),
                storageRootOf(account).toString()));
  }

  private Optional<Hash> codeHash(final Hash blockHash, final Address address) {
    return account(blockHash, address, Account::getCodeHash);
  }

  private <U> Optional<U> account(
      final Hash blockHash, final Address address, final Function<Account, U> mapper) {
    return blockchainQueries.getAndMapWorldState(
        blockHash, worldState -> Optional.ofNullable(worldState.get(address)).map(mapper));
  }

  private static Hash storageRootOf(final Account account) {
    return account instanceof AccountValue value ? value.getStorageRoot() : Hash.EMPTY_TRIE_HASH;
  }

  private Set<String> everyBlockUpTo(final long endBlock) {
    final Set<String> reported = new LinkedHashSet<>();
    for (long number = 1; number <= endBlock; number++) {
      reported.addAll(result(BY_NUMBER, hex(number)));
    }
    return reported;
  }

  private List<String> sendersUpTo(final long endBlock) {
    final List<String> senders = new ArrayList<>();
    for (long number = 1; number <= endBlock; number++) {
      block(number)
          .getBody()
          .getTransactions()
          .forEach(transaction -> senders.add(transaction.getSender().toString()));
    }
    return senders;
  }

  private Block firstBlockWithTransactions() {
    return firstBlockMatching(
        block -> !block.getBody().getTransactions().isEmpty(), "transactions");
  }

  private Block firstBlockWithContractCreation() {
    return firstBlockMatching(
        block ->
            block.getBody().getTransactions().stream().anyMatch(Transaction::isContractCreation),
        "a contract creation");
  }

  private Block firstBlockMatching(final Predicate<Block> predicate, final String description) {
    for (long number = 1; number <= chainHead; number++) {
      final Block block = block(number);
      if (predicate.test(block)) {
        return block;
      }
    }
    throw new IllegalStateException("the test chain has no block with " + description);
  }

  private Block block(final long number) {
    return blockchain.getBlockByNumber(number).orElseThrow();
  }

  private BlockHeader header(final long number) {
    return blockchain.getBlockHeader(number).orElseThrow();
  }

  @SuppressWarnings("unchecked")
  private List<String> result(final String method, final Object... params) {
    final JsonRpcResponse response = response(method, params);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    return (List<String>) ((JsonRpcSuccessResponse) response).getResult();
  }

  private String error(final String method, final Object... params) {
    final JsonRpcErrorResponse response = errorResponse(method, params);
    assertThat(response.getError().getCode()).isEqualTo(-32000);
    return response.getError().getMessage();
  }

  private JsonRpcErrorResponse errorResponse(final String method, final Object... params) {
    final JsonRpcResponse response = response(method, params);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    return (JsonRpcErrorResponse) response;
  }

  private JsonRpcResponse response(final String method, final Object... params) {
    return methods
        .get(method)
        .response(new JsonRpcRequestContext(new JsonRpcRequest("2.0", method, params)));
  }

  private static String hex(final long blockNumber) {
    return "0x" + Long.toHexString(blockNumber);
  }
}
