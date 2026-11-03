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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.FilterParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.Trace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTraceGenerator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.RewardTraceGenerator;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

public class TraceFlatTransactionStep
    implements Function<TransactionTrace, CompletableFuture<Stream<FlatTrace>>> {

  private final ProtocolSchedule protocolSchedule;
  private final Block block;
  private final Optional<FilterParameter> filterParameter;

  public TraceFlatTransactionStep(
      final ProtocolSchedule protocolSchedule,
      final Block block,
      final Optional<FilterParameter> filterParameter) {
    this.protocolSchedule = protocolSchedule;
    this.block = block;
    this.filterParameter = filterParameter;
  }

  @Override
  public CompletableFuture<Stream<FlatTrace>> apply(final TransactionTrace transactionTrace) {
    Stream<Trace> traceStream = null;
    Block block = this.block;
    if (block == null) block = transactionTrace.getBlock().get();
    if (transactionTrace.getTransaction() == null) {
      traceStream = RewardTraceGenerator.generateFromBlock(protocolSchedule, block);
    } else {
      traceStream =
          FlatTraceGenerator.generateFromTransactionTraceAndBlock(
              protocolSchedule, transactionTrace, block);
    }
    if (filterParameter.isPresent()) {
      return CompletableFuture.completedFuture(
          traceStream
              .map(FlatTrace.class::cast)
              .filter(
                  trace ->
                      filterParameter
                          .get()
                          .matchesTraceAddresses(
                              Optional.ofNullable(trace.getAction().getFrom())
                                  .map(Address::fromHexString),
                              // a reward has no recipient but its author
                              Optional.ofNullable(trace.getAction().getTo())
                                  .or(() -> Optional.ofNullable(trace.getAction().getAuthor()))
                                  .map(Address::fromHexString))));

    } else {
      return CompletableFuture.completedFuture(traceStream.map(FlatTrace.class::cast));
    }
  }
}
