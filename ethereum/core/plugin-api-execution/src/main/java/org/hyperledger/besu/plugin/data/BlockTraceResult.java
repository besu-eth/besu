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
package org.hyperledger.besu.plugin.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Represents the result of tracing a block, containing information about the transaction traces.
 */
public class BlockTraceResult {
  final List<TransactionTraceResult> transactionTraceResults;

  /**
   * Constructs a BlockTraceResult with the given list of transaction trace results.
   *
   * @param transactionTraceResults The list of transaction trace results to be associated with this
   *     block.
   */
  public BlockTraceResult(final List<TransactionTraceResult> transactionTraceResults) {
    this.transactionTraceResults = transactionTraceResults;
  }

  /**
   * Creates an empty BlockTraceResult with no transaction trace results.
   *
   * @return An empty BlockTraceResult.
   */
  public static BlockTraceResult empty() {
    return new BlockTraceResult(new ArrayList<>());
  }

  /**
   * Get the list of transaction trace results for this block.
   *
   * @return The list of transaction trace results.
   */
  public List<TransactionTraceResult> transactionTraceResults() {
    return transactionTraceResults;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final BlockTraceResult that = (BlockTraceResult) o;
    return transactionTraceResults.equals(that.transactionTraceResults());
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionTraceResults);
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("BlockTraceResult{transactionTraceResults=[");

    final Iterator<TransactionTraceResult> iterator = transactionTraceResults.iterator();
    while (iterator.hasNext()) {
      builder.append(iterator.next().toString());

      if (iterator.hasNext()) {
        builder.append(",");
      }
    }
    builder.append("]}");
    return builder.toString();
  }

  /**
   * Creates a new builder to construct a BlockTraceResult.
   *
   * @return A new BlockTraceResult.Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** A builder class for constructing a BlockTraceResult. */
  public static class Builder {
    List<TransactionTraceResult> transactionTraceResults = new ArrayList<>();

    /** Constructs a new builder instance. */
    private Builder() {}

    /**
     * Adds a transaction trace result to the builder.
     *
     * @param transactionTraceResult The transaction trace result to add to the builder.
     * @return This builder instance, for method chaining.
     */
    public Builder addTransactionTraceResult(final TransactionTraceResult transactionTraceResult) {
      transactionTraceResults.add(transactionTraceResult);

      return this;
    }

    /**
     * Constructs a BlockTraceResult using the transaction trace results added to the builder.
     *
     * @return A BlockTraceResult containing the added transaction trace results.
     */
    public BlockTraceResult build() {
      return new BlockTraceResult(transactionTraceResults);
    }
  }
}
