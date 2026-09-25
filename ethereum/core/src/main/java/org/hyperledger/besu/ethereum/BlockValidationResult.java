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
   * Whether the failure lies with this node rather than with the block: a storage or trie fault
   * says nothing about the block's validity, any other failure does.
   *
   * @return true if the failure was caused by a local storage or trie fault
   */
  public boolean isLocalFailure() {
    return cause
        .map(
            throwable ->
                throwable instanceof StorageException || throwable instanceof MerkleTrieException)
        .orElse(false);
  }
}
