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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class RpcErrorTypeConverterTest {

  public static Collection<Object[]> expectedErrorMapping() {
    return Arrays.asList(
        new Object[][] {
          {TransactionInvalidReason.NONCE_TOO_LOW, RpcErrorType.NONCE_TOO_LOW},
          {TransactionInvalidReason.NONCE_TOO_HIGH, RpcErrorType.NONCE_TOO_HIGH},
          {TransactionInvalidReason.INVALID_SIGNATURE, RpcErrorType.INVALID_TRANSACTION_SIGNATURE},
          {
            TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT,
            RpcErrorType.INTRINSIC_GAS_EXCEEDS_LIMIT
          },
          {
            TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE,
            RpcErrorType.TRANSACTION_UPFRONT_COST_EXCEEDS_BALANCE
          },
          {TransactionInvalidReason.EXCEEDS_BLOCK_GAS_LIMIT, RpcErrorType.EXCEEDS_BLOCK_GAS_LIMIT},
          {TransactionInvalidReason.WRONG_CHAIN_ID, RpcErrorType.WRONG_CHAIN_ID},
          {
            TransactionInvalidReason.REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED,
            RpcErrorType.REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED
          },
          {
            TransactionInvalidReason.TX_SENDER_NOT_AUTHORIZED, RpcErrorType.TX_SENDER_NOT_AUTHORIZED
          },
          {
            TransactionInvalidReason.CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE,
            RpcErrorType.CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE
          },
          {TransactionInvalidReason.GAS_PRICE_TOO_LOW, RpcErrorType.GAS_PRICE_TOO_LOW},
          {
            TransactionInvalidReason.TRANSACTION_ALREADY_KNOWN,
            RpcErrorType.ETH_SEND_TX_ALREADY_KNOWN
          },
          {
            TransactionInvalidReason.TRANSACTION_REPLACEMENT_UNDERPRICED,
            RpcErrorType.ETH_SEND_TX_REPLACEMENT_UNDERPRICED
          },
          {
            TransactionInvalidReason.BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE,
            RpcErrorType.BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE
          },
          {TransactionInvalidReason.TOTAL_BLOB_GAS_TOO_HIGH, RpcErrorType.TOTAL_BLOB_GAS_TOO_HIGH},
          {TransactionInvalidReason.INVALID_BLOBS, RpcErrorType.INVALID_BLOBS}
        });
  }

  @ParameterizedTest
  @MethodSource("expectedErrorMapping")
  public void expectedTransactionValidationToJsonRpcErrorConversion(
      final TransactionInvalidReason txInvalidReason, final RpcErrorType expectedJsonRpcError) {
    assertThat(JsonRpcErrorConverter.convertTransactionInvalidReason(txInvalidReason))
        .isEqualTo(expectedJsonRpcError);
  }
}
