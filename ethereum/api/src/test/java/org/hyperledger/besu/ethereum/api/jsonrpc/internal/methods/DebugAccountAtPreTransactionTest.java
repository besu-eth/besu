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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockReplay;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugAccountAtResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * debug_accountAt must describe the account before the selected transaction. The sender nonce in
 * that state is the nonce of the transaction, not that nonce plus one.
 */
class DebugAccountAtPreTransactionTest {

  private static final KeyPair SENDER_KEYS =
      new KeyPair(
          SECPPrivateKey.create(
              Bytes32.fromHexString(
                  "0x45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8"),
              "ECDSA"),
          SECPPublicKey.create(
              Bytes.fromHexString(
                  "0x3a514176466fa815ed481ffad09110a2d344f6c9b78c1d14afc351c3a51be33d8072e77939dc03ba44790779b7a1025baf3003f6732430e20cd9b76d953391b3"),
              "ECDSA"));
  private static final Address SENDER = Address.extract(SENDER_KEYS.getPublicKey());
  private static final Address FIRST_RECIPIENT =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address SECOND_RECIPIENT =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");
  private static final long TRANSFER_GAS = 21_000L;

  private BlockchainSetupUtil setup;

  @AfterEach
  void stopScheduler() {
    if (setup != null) {
      setup.getScheduler().stop();
      setup = null;
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = DataStorageFormat.class,
      names = {"FOREST", "BONSAI"})
  void senderAtIndexZeroMatchesParentState(final DataStorageFormat format) {
    final Chain chain = importChain(format);
    boolean checked = false;
    for (final Block block : chain.blocksWithTransactions()) {
      final Transaction transaction = block.getBody().getTransactions().get(0);
      assertThat(transaction.getSender()).isEqualTo(SENDER);
      final AccountAtResult actual = chain.accountAt(block.getHash(), 0, SENDER);
      final AccountSnapshot parent = chain.accountAtParent(block, SENDER);
      assertThat(parent).isNotNull();
      final SoftAssertions softly = new SoftAssertions();
      softly
          .assertThat(actual.nonce())
          .as("block %s tx 0 sender nonce", block.getHeader().getNumber())
          .isEqualTo(parent.nonce());
      softly
          .assertThat(actual.balance())
          .as("block %s tx 0 sender balance", block.getHeader().getNumber())
          .isEqualTo(parent.balance());
      softly
          .assertThat(actual.code())
          .as("block %s tx 0 sender code", block.getHeader().getNumber())
          .isEqualTo(parent.code());
      softly
          .assertThat(actual.codehash())
          .as("block %s tx 0 sender codehash", block.getHeader().getNumber())
          .isEqualTo(parent.codehash());
      softly.assertAll();
      checked = true;
    }
    assertThat(checked).isTrue();
  }

  @ParameterizedTest
  @EnumSource(
      value = DataStorageFormat.class,
      names = {"FOREST", "BONSAI"})
  void accountThatIsNotTheSenderKeepsParentValues(final DataStorageFormat format) {
    final Chain chain = importChain(format);
    boolean checked = false;
    for (final Block block : chain.blocksWithTransactions()) {
      final Transaction transaction = block.getBody().getTransactions().get(0);
      if (transaction.getTo().isEmpty()) {
        continue;
      }
      final Address recipient = transaction.getTo().get();
      final AccountSnapshot parent = chain.accountAtParent(block, recipient);
      if (parent == null) {
        continue;
      }
      final AccountAtResult actual = chain.accountAt(block.getHash(), 0, recipient);
      final SoftAssertions softly = new SoftAssertions();
      softly
          .assertThat(actual.nonce())
          .as("block %s recipient nonce", block.getHeader().getNumber())
          .isEqualTo(parent.nonce());
      softly
          .assertThat(actual.balance())
          .as("block %s recipient balance", block.getHeader().getNumber())
          .isEqualTo(parent.balance());
      softly
          .assertThat(actual.code())
          .as("block %s recipient code", block.getHeader().getNumber())
          .isEqualTo(parent.code());
      softly
          .assertThat(actual.codehash())
          .as("block %s recipient codehash", block.getHeader().getNumber())
          .isEqualTo(parent.codehash());
      softly.assertAll();
      checked = true;
    }
    assertThat(checked).as("a called account that already exists before the transaction").isTrue();
  }

  @ParameterizedTest
  @EnumSource(
      value = DataStorageFormat.class,
      names = {"FOREST", "BONSAI"})
  void secondTransactionSeesTheFirstAndNotItself(final DataStorageFormat format) {
    final Chain chain = importChain(format);
    final AccountSnapshot parent = chain.accountAtBlock(chain.headHash(), SENDER);
    assertThat(parent).isNotNull();
    assertThat(chain.accountAtBlock(chain.headHash(), FIRST_RECIPIENT)).isNull();

    final Transaction first = transfer(parent.nonceValue(), FIRST_RECIPIENT, Wei.of(1));
    final Transaction second = transfer(parent.nonceValue() + 1, SECOND_RECIPIENT, Wei.of(2));
    final Block block = appendTwoTransactionBlock(chain, first, second);

    final AccountAtResult senderBeforeFirst = chain.accountAt(block.getHash(), 0, SENDER);
    assertThat(senderBeforeFirst.nonce())
        .as("index 0 uses the parent state")
        .isEqualTo(parent.nonce());
    assertThat(senderBeforeFirst.balance()).isEqualTo(parent.balance());
    assertThat(senderBeforeFirst.code()).isEqualTo(parent.code());
    assertThat(senderBeforeFirst.codehash()).isEqualTo(parent.codehash());

    final JsonRpcResponse missingRecipient = chain.response(block.getHash(), 0, FIRST_RECIPIENT);
    assertThat(missingRecipient).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) missingRecipient).getErrorType())
        .isEqualTo(RpcErrorType.NO_ACCOUNT_FOUND);

    final AccountAtResult senderBeforeSecond = chain.accountAt(block.getHash(), 1, SENDER);
    final Wei gasAndValue = first.getUpfrontGasCost(Wei.of(1), Wei.ZERO, 0L).add(first.getValue());
    assertThat(senderBeforeSecond.nonce())
        .as("index 1 includes the preceding transaction and not itself")
        .isEqualTo(Quantity.create(parent.nonceValue() + 1));
    assertThat(senderBeforeSecond.balance())
        .isEqualTo(Quantity.create(parent.balanceValue().subtract(gasAndValue)));

    final AccountAtResult recipientBeforeSecond =
        chain.accountAt(block.getHash(), 1, FIRST_RECIPIENT);
    assertThat(recipientBeforeSecond.nonce()).isEqualTo("0x0");
    assertThat(recipientBeforeSecond.balance()).isEqualTo(Quantity.create(first.getValue()));
    assertThat(recipientBeforeSecond.code()).isEqualTo("0x0");
  }

  private Chain importChain(final DataStorageFormat format) {
    setup = BlockchainSetupUtil.forTesting(format);
    setup.importAllBlocks();
    final BlockchainQueries queries =
        new BlockchainQueries(
            setup.getProtocolSchedule(),
            setup.getBlockchain(),
            setup.getWorldArchive(),
            MiningConfiguration.newDefault());
    final BlockReplay blockReplay =
        new BlockReplay(
            setup.getProtocolSchedule(), setup.getProtocolContext(), setup.getBlockchain());
    final DebugAccountAt method = new DebugAccountAt(queries, blockReplay);
    return new Chain(queries, method);
  }

  private Block appendTwoTransactionBlock(
      final Chain chain, final Transaction first, final Transaction second) {
    final BlockHeader parent = setup.getBlockchain().getChainHeadHeader();
    final List<Transaction> transactions = List.of(first, second);
    final BlockBody body = new BlockBody(transactions, List.of());
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .parentHash(parent.getHash())
            .number(parent.getNumber() + 1)
            .timestamp(parent.getTimestamp() + 1)
            .gasLimit(1_000_000L)
            .difficulty(Difficulty.ONE)
            .ommersHash(Hash.EMPTY_LIST_HASH)
            .transactionsRoot(BodyValidation.transactionsRoot(transactions))
            .coinbase(parent.getCoinbase())
            .buildHeader();
    final Block block = new Block(header, body);
    final TransactionReceipt receipt =
        new TransactionReceipt(Hash.EMPTY_TRIE_HASH, TRANSFER_GAS, List.of(), Optional.empty());
    setup.getBlockchain().appendBlock(block, List.of(receipt, receipt));
    assertThat(chain.queries.blockByHash(block.getHash())).isPresent();
    return block;
  }

  private static Transaction transfer(final long nonce, final Address to, final Wei value) {
    return Transaction.builder()
        .type(org.hyperledger.besu.datatypes.TransactionType.FRONTIER)
        .nonce(nonce)
        .gasPrice(Wei.of(1))
        .gasLimit(TRANSFER_GAS)
        .to(to)
        .value(value)
        .payload(Bytes.EMPTY)
        .signAndBuild(SENDER_KEYS);
  }

  private record AccountSnapshot(
      String code,
      String nonce,
      String balance,
      String codehash,
      long nonceValue,
      Wei balanceValue) {}

  private record AccountAtResult(String code, String nonce, String balance, String codehash) {}

  private final class Chain {
    private final BlockchainQueries queries;
    private final DebugAccountAt method;

    private Chain(final BlockchainQueries queries, final DebugAccountAt method) {
      this.queries = queries;
      this.method = method;
    }

    private Hash headHash() {
      return setup.getBlockchain().getChainHeadHash();
    }

    private List<Block> blocksWithTransactions() {
      return setup.getBlocks().stream()
          .filter(block -> !block.getBody().getTransactions().isEmpty())
          .toList();
    }

    private AccountSnapshot accountAtParent(final Block block, final Address address) {
      return accountAtBlock(block.getHeader().getParentHash(), address);
    }

    private AccountSnapshot accountAtBlock(final Hash blockHash, final Address address) {
      return queries
          .getAndMapWorldState(
              blockHash,
              worldState -> {
                final Account account = worldState.get(address);
                if (account == null) {
                  return Optional.empty();
                }
                final Bytes code = account.getCode();
                return Optional.of(
                    new AccountSnapshot(
                        code.isEmpty() ? "0x0" : code.toHexString(),
                        Quantity.create(account.getNonce()),
                        Quantity.create(account.getBalance()),
                        Quantity.create(account.getCodeHash().getBytes()),
                        account.getNonce(),
                        account.getBalance()));
              })
          .orElse(null);
    }

    private AccountAtResult accountAt(
        final Hash blockHash, final int index, final Address address) {
      final JsonRpcResponse response = response(blockHash, index, address);
      assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
      final DebugAccountAtResult result =
          (DebugAccountAtResult) ((JsonRpcSuccessResponse) response).getResult();
      return new AccountAtResult(
          result.getCode(), result.getNonce(), result.getBalance(), result.getCodehash());
    }

    private JsonRpcResponse response(final Hash blockHash, final int index, final Address address) {
      final Object[] params = new Object[] {blockHash.toHexString(), index, address.toHexString()};
      return method.response(
          new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_accountAt", params)));
    }
  }
}
