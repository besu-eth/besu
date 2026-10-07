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
package org.hyperledger.besu.ethereum;

import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;

import com.google.common.base.Throwables;

/**
 * Represents the result of a block validation. This class holds the success status, error message,
 * and cause of the validation.
 */
public class BlockValidationResult {

  /** The error message of the failed validation, if any. */
  public final Optional<String> errorMessage;

  /** The cause of the failed validation, if any. */
  public final Optional<Throwable> cause;

  /**
   * The success status of the validation. True if the validation was successful, false otherwise.
   */
  public final boolean success;

  /** Constructs a new BlockValidationResult indicating a successful validation. */
  public BlockValidationResult() {
    this.success = true;
    this.errorMessage = Optional.empty();
    this.cause = Optional.empty();
  }

  /**
   * Constructs a new BlockValidationResult indicating a failed validation with the given error
   * message.
   *
   * @param errorMessage the error message of the failed validation
   */
  public BlockValidationResult(final String errorMessage) {
    this.success = false;
    this.errorMessage = Optional.of(errorMessage);
    this.cause = Optional.empty();
  }

  /**
   * Constructs a new BlockValidationResult indicating a failed validation with the given error
   * message and cause.
   *
   * @param errorMessage the error message of the failed validation
   * @param cause the cause of the failed validation
   */
  public BlockValidationResult(final String errorMessage, final Throwable cause) {
    this.success = false;
    this.errorMessage = Optional.of(errorMessage);
    this.cause = Optional.of(cause);
  }

  /**
   * Checks if the validation was successful.
   *
   * @return true if the validation was successful, false otherwise
   */
  public boolean isSuccessful() {
    return this.success;
  }

  /**
   * Checks if the validation failed.
   *
   * @return true if the validation failed, false otherwise
   */
  public boolean isFailed() {
    return !isSuccessful();
  }

  /**
   * Gets the cause of the failed validation.
   *
   * @return the cause of the failed validation
   */
  public Optional<Throwable> causedBy() {
    return cause;
  }

  /**
   * Whether the failure lies with this node rather than with the block: a storage or trie fault, or
   * processing that was interrupted, cancelled or rejected by a shut down executor, says nothing
   * about the block's validity, any other failure does.
   *
   * @return true if the failure was caused by this node
   */
  public boolean isLocalFailure() {
    return cause.map(BlockValidationResult::isLocalFailure).orElse(false);
  }

  /**
   * Whether a throwable denotes a fault of this node rather than of the block being processed. A
   * fault raised on a worker thread arrives wrapped, so the whole causal chain is inspected.
   *
   * @param throwable the throwable
   * @return true for a storage or trie fault, an interruption, a cancellation or a rejected task
   */
  public static boolean isLocalFailure(final Throwable throwable) {
    return Throwables.getCausalChain(throwable).stream()
        .anyMatch(
            cause ->
                isStorageFault(cause)
                    || cause instanceof InterruptedException
                    || cause instanceof CancellationException
                    || cause instanceof RejectedExecutionException);
  }

  /**
   * Whether a throwable is caused by a storage or trie fault, which a retry can get past, unlike an
   * interruption that would only be hit again.
   *
   * @param throwable the throwable
   * @return true for a storage or trie fault anywhere in the causal chain
   */
  public static boolean isStorageFailure(final Throwable throwable) {
    return Throwables.getCausalChain(throwable).stream()
        .anyMatch(BlockValidationResult::isStorageFault);
  }

  private static boolean isStorageFault(final Throwable throwable) {
    return throwable instanceof StorageException || throwable instanceof MerkleTrieException;
  }
}
