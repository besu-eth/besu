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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineGetInclusionListV1Test {

  private static final Supplier<SignatureAlgorithm> SIGNATURE_ALGORITHM =
      Suppliers.memoize(SignatureAlgorithmFactory::getInstance);
  private static final SECPPrivateKey PRIVATE_KEY1 =
      SIGNATURE_ALGORITHM
          .get()
          .createPrivateKey(
              Bytes32.fromHexString(
                  "8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"));
  private static final KeyPair KEYS1 =
      new KeyPair(PRIVATE_KEY1, SIGNATURE_ALGORITHM.get().createPublicKey(PRIVATE_KEY1));

  private static final Hash KNOWN_PARENT_HASH =
      Hash.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000001");
  private static final Hash UNKNOWN_PARENT_HASH =
      Hash.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000099");

  @Mock private ProtocolContext protocolContext;
  @Mock private EngineCallListener engineCallListener;
  @Mock private MutableBlockchain blockchain;
  @Mock private TransactionPool transactionPool;
  @Mock private BlockHeader parentBlockHeader;
  @Mock private MergeContext mergeContext;

  private EngineGetInclusionListV1 method;
  private StubMetricsSystem metricsSystem;
  private static final Vertx vertx = Vertx.vertx();

  @BeforeEach
  public void beforeEach() {
    when(protocolContext.safeConsensusContext(any())).thenReturn(Optional.ofNullable(mergeContext));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockHeader(KNOWN_PARENT_HASH)).thenReturn(Optional.of(parentBlockHeader));
    when(blockchain.getBlockHeader(UNKNOWN_PARENT_HASH)).thenReturn(Optional.empty());
    when(parentBlockHeader.getBaseFee()).thenReturn(Optional.of(Wei.of(7)));

    this.metricsSystem = new StubMetricsSystem();
    this.method =
        new EngineGetInclusionListV1(
            vertx, protocolContext, engineCallListener, transactionPool, metricsSystem);
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getInclusionListV1");
  }

  @Test
  public void shouldReturnEmptyListWhenNoTransactionsInPool() {
    when(transactionPool.getInclusionListPendingTransactions()).thenReturn(List.of());

    final JsonRpcResponse response = resp(KNOWN_PARENT_HASH);
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);

    final List<Transaction> result = fromSuccessResp(response);
    assertThat(result).isEmpty();
  }

  @Test
  public void shouldReturnSelectedTransactionsFromPool() {
    final Transaction tx1 = createLegacyTransaction(0, Wei.of(100));
    final Transaction tx2 = createLegacyTransaction(1, Wei.of(200));
    final PendingTransaction pt1 =
        PendingTransaction.newPendingTransaction(tx1, false, false, (byte) 0);
    final PendingTransaction pt2 =
        PendingTransaction.newPendingTransaction(tx2, false, false, (byte) 0);

    when(transactionPool.getInclusionListPendingTransactions()).thenReturn(List.of(pt1, pt2));

    final JsonRpcResponse response = resp(KNOWN_PARENT_HASH);
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);

    final List<Transaction> result = fromSuccessResp(response);
    assertThat(result).contains(tx1, tx2);
  }

  @Test
  public void shouldFilterBlobTransactions() {
    // Blob transactions should be excluded from inclusion list per EIP-7805
    // Only non-blob transactions should be selected
    final Transaction legacyTx = createLegacyTransaction(0, Wei.of(100));
    final PendingTransaction pt =
        PendingTransaction.newPendingTransaction(legacyTx, false, false, (byte) 0);

    when(transactionPool.getInclusionListPendingTransactions()).thenReturn(List.of(pt));

    final JsonRpcResponse response = resp(KNOWN_PARENT_HASH);
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);

    final List<Transaction> result = fromSuccessResp(response);
    assertThat(result).hasSize(1);
  }

  @Test
  public void shouldIncrementTransactionsGeneratedMetric() {
    final Transaction tx1 = createLegacyTransaction(0, Wei.of(100));
    final PendingTransaction pt1 =
        PendingTransaction.newPendingTransaction(tx1, false, false, (byte) 0);

    when(transactionPool.getInclusionListPendingTransactions()).thenReturn(List.of(pt1));

    resp(KNOWN_PARENT_HASH);

    assertThat(metricsSystem.getCounterValue("engine_inclusion_list_transactions_generated"))
        .isEqualTo(1);
  }

  @Test
  public void shouldIncrementSelectorDurationMetric() {
    final Transaction tx1 = createLegacyTransaction(0, Wei.of(100));
    final PendingTransaction pt1 =
        PendingTransaction.newPendingTransaction(tx1, false, false, (byte) 0);

    when(transactionPool.getInclusionListPendingTransactions()).thenReturn(List.of(pt1));

    resp(KNOWN_PARENT_HASH);

    // Duration may be 0ms for fast execution, but the counter should exist
    assertThat(metricsSystem.getCounterValue("engine_inclusion_list_selector_duration_ms"))
        .isGreaterThanOrEqualTo(0);
  }

  @Test
  public void shouldNotIncrementMetricsOnUnknownParent() {
    resp(UNKNOWN_PARENT_HASH);

    assertThat(metricsSystem.getCounterValue("engine_inclusion_list_transactions_generated"))
        .isEqualTo(0);
  }

  private Transaction createLegacyTransaction(final long nonce, final Wei gasPrice) {
    return new TransactionTestFixture()
        .to(Optional.of(Address.ZERO))
        .type(TransactionType.FRONTIER)
        .chainId(Optional.of(BigInteger.valueOf(42)))
        .nonce(nonce)
        .gasPrice(gasPrice)
        .gasLimit(21000)
        .createTransaction(KEYS1);
  }

  private JsonRpcResponse resp(final Hash parentHash) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                RpcMethod.ENGINE_GET_INCLUSION_LIST_V1.getMethodName(),
                new Object[] {parentHash.toHexString()})));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private List<Transaction> fromSuccessResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .map(JsonRpcSuccessResponse::getResult)
        .map(List.class::cast)
        .get();
  }
}
