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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugBatchSendRawTransaction.ExecutionStatus;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
public class DebugBatchSendRawTransactionTest {

  private static final String NAME = RpcMethod.DEBUG_BATCH_RAW_TRANSACTION.getMethodName();
  private static final String VERSION = "2.0";
  @Mock TransactionPool transactionPool;
  DebugBatchSendRawTransaction method;

  @BeforeEach
  public void setUp() {
    method = new DebugBatchSendRawTransaction(transactionPool);
  }

  @Test
  public void nominalCaseSingleTransaction() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                VERSION,
                NAME,
                new Object[] {
                  "0xf868808203e882520894627306090abab3a6e1400e9345bc60c78a8bef57872386f26fc10000801ba0ac74ecfa0e9b85785f042c143ead4780931234cc9a032fce99fab1f45e0d90faa02fd17e8eb433d4ca47727653232045d4f81322619c0852d3fe8ddcfcedb66a43"
                }));
    when(transactionPool.addTransactionViaApi(any(Transaction.class)))
        .thenReturn(ValidationResult.valid());
    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    assertThat(response).isNotNull();
    final List<ExecutionStatus> result = (List<ExecutionStatus>) response.getResult();
    assertThat(result).isNotNull().hasSize(1).containsExactly(new ExecutionStatus(0));
  }

  @Test
  public void nominalCaseMultipleTransactions() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                VERSION,
                NAME,
                new Object[] {
                  "0xf868808203e882520894627306090abab3a6e1400e9345bc60c78a8bef57872386f26fc10000801ba0ac74ecfa0e9b85785f042c143ead4780931234cc9a032fce99fab1f45e0d90faa02fd17e8eb433d4ca47727653232045d4f81322619c0852d3fe8ddcfcedb66a43",
                  "0xf868018203e882520894627306090abab3a6e1400e9345bc60c78a8bef57876a94d74f430000801ba092faeec7bcb7418a79cd9f74c739237d72d52b5ab25aa08e332053304456e129a0386e3e9205a3553ecc5e85fc753c93196484c0fdeaaacdd61425caeb11bc6e5a"
                }));
    when(transactionPool.addTransactionViaApi(any(Transaction.class)))
        .thenReturn(ValidationResult.valid());
    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    assertThat(response).isNotNull();
    final List<ExecutionStatus> result = (List<ExecutionStatus>) response.getResult();
    assertThat(result)
        .isNotNull()
        .hasSize(2)
        .containsExactly(new ExecutionStatus(0), new ExecutionStatus(1));
  }

  @Test
  public void errorSingleTransactionHexParsing() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest(VERSION, NAME, new Object[] {"www"}));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    assertThat(response).isNotNull();
    final List<ExecutionStatus> result = (List<ExecutionStatus>) response.getResult();
    assertThat(result)
        .isNotNull()
        .hasSize(1)
        .containsExactly(new ExecutionStatus(0, false, "Invalid raw transaction hex"));
  }

  @Test
  public void errorSingleTransactionSignatureInvalidV() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                VERSION,
                NAME,
                new Object[] {
                  "0xf868808203e882520894627306090abab3a6e1400e9345bc60c78a8bef57872386f26fc10000801fa0ac74ecfa0e9b85785f042c143ead4780931234cc9a032fce99fab1f45e0d90faa02fd17e8eb433d4ca47727653232045d4f81322619c0852d3fe8ddcfcedb66a43"
                }));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    assertThat(response).isNotNull();
    final List<ExecutionStatus> result = (List<ExecutionStatus>) response.getResult();
    assertThat(result)
        .isNotNull()
        .hasSize(1)
        .containsExactly(new ExecutionStatus(0, false, "Invalid RLP in raw transaction hex"));
  }
}
