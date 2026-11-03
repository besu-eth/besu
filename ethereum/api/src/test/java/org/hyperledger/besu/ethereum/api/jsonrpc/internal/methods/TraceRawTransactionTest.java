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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TraceRawTransactionTest {

  private static final String RAW_TX =
      "0xf8e21e81ef83fffff294001000000000000000000000000000000000000080b88000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000001ba067e3f9241dd0288a0d33e3e33361a8854469830cd3833c22fa44ac55884a1387a045c3adb00d0f301c99d5349d30948e4cbdb9016d503ec49c37af86324110cab8";

  @Mock private BlockchainQueries blockchainQueries;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private TransactionSimulator transactionSimulator;
  @Mock private ApiConfiguration apiConfiguration;
  @Mock private BlockHeader headBlock;

  @Test
  public void serverStepLimitClampsTheTracer() {
    when(apiConfiguration.getDebugTraceStepLimit()).thenReturn(500L);
    when(blockchainQueries.headBlockHeader()).thenReturn(headBlock);
    final TraceRawTransaction method =
        new TraceRawTransaction(
            protocolSchedule, blockchainQueries, transactionSimulator, apiConfiguration);

    method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "trace_rawTransaction", new Object[] {RAW_TX, new String[] {"vmTrace"}})));

    final ArgumentCaptor<OperationTracer> tracer = ArgumentCaptor.forClass(OperationTracer.class);
    verify(transactionSimulator)
        .process(any(), any(TransactionValidationParams.class), tracer.capture(), any(), any());
    assertThat(((DebugOperationTracer) tracer.getValue()).getConfig().limit()).isEqualTo(500);
  }
}
