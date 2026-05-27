/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.evmtool;

import com.google.common.base.Splitter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Maps a Besu engine-API validation error message to the EEST exception name(s) it represents.
 *
 * <p>This is a Java port of the {@code BesuExceptionMapper} in execution-spec-tests
 * ({@code clis/besu.py}). It lets {@code engine-test} reproduce hive's strict exception matching:
 * when a fixture expects an {@code INVALID} payload with a specific exception, the message Besu
 * returns must map to that exception.
 */
final class EngineTestExceptionMapper {

  private EngineTestExceptionMapper() {}

  private record Mapping(String exception, Pattern pattern) {}

  private static Mapping substring(final String exception, final String literal) {
    return new Mapping(exception, Pattern.compile(Pattern.quote(literal)));
  }

  private static Mapping regex(final String exception, final String regex) {
    return new Mapping(exception, Pattern.compile(regex));
  }

  // Ported verbatim from BesuExceptionMapper (mapping_substring + mapping_regex). Order is not
  // significant; a message may match several patterns (e.g. multiple BAL exceptions).
  private static final List<Mapping> MAPPINGS =
      List.of(
          substring("TransactionException.NONCE_IS_MAX", "invalid Nonce must be less than"),
          substring(
              "TransactionException.INSUFFICIENT_MAX_FEE_PER_BLOB_GAS",
              "transaction invalid tx max fee per blob gas less than block blob gas fee"),
          substring(
              "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW",
              "invalid Upfront gas cost cannot exceed 2^256 Wei"),
          substring(
              "TransactionException.INSUFFICIENT_MAX_FEE_PER_GAS",
              "transaction invalid gasPrice is less than the current BaseFee"),
          substring("BlockException.GAS_USED_OVERFLOW", "provided gas insufficient"),
          substring("TransactionException.GAS_ALLOWANCE_EXCEEDED", "provided gas insufficient"),
          substring(
              "TransactionException.PRIORITY_GREATER_THAN_MAX_FEE_PER_GAS",
              "transaction invalid max priority fee per gas cannot be greater than max fee per gas"),
          substring(
              "TransactionException.TYPE_3_TX_INVALID_BLOB_VERSIONED_HASH", "Invalid versionedHash"),
          substring(
              "TransactionException.TYPE_3_TX_CONTRACT_CREATION",
              "transaction invalid transaction blob transactions must have a to address"),
          substring(
              "TransactionException.TYPE_3_TX_WITH_FULL_BLOBS",
              "Failed to decode transactions from block parameter"),
          substring(
              "TransactionException.TYPE_3_TX_ZERO_BLOBS",
              "Failed to decode transactions from block parameter"),
          substring(
              "TransactionException.TYPE_3_TX_PRE_FORK",
              "Transaction type BLOB is invalid, accepted transaction types are"),
          substring(
              "TransactionException.TYPE_4_EMPTY_AUTHORIZATION_LIST",
              "transaction invalid transaction code delegation transactions must have a non-empty"
                  + " code delegation list"),
          substring(
              "TransactionException.TYPE_4_TX_CONTRACT_CREATION",
              "transaction invalid transaction code delegation transactions must have a to address"),
          substring(
              "TransactionException.TYPE_4_TX_PRE_FORK",
              "transaction invalid Transaction type DELEGATE_CODE is invalid"),
          substring(
              "BlockException.RLP_STRUCTURES_ENCODING",
              "Failed to decode transactions from block parameter"),
          substring(
              "BlockException.INCORRECT_EXCESS_BLOB_GAS",
              "Payload excessBlobGas does not match calculated excessBlobGas"),
          substring(
              "BlockException.BLOB_GAS_USED_ABOVE_LIMIT",
              "Payload BlobGasUsed does not match calculated BlobGasUsed"),
          substring(
              "BlockException.INCORRECT_BLOB_GAS_USED",
              "Payload BlobGasUsed does not match calculated BlobGasUsed"),
          substring("BlockException.INVALID_GAS_USED_ABOVE_LIMIT", "Header validation failed (FULL)"),
          substring("BlockException.INVALID_GASLIMIT", "Header validation failed (FULL)"),
          substring("BlockException.EXTRA_DATA_TOO_BIG", "Header validation failed (FULL)"),
          substring("BlockException.INVALID_BLOCK_NUMBER", "Header validation failed (FULL)"),
          substring("BlockException.INVALID_BASEFEE_PER_GAS", "Header validation failed (FULL)"),
          substring(
              "BlockException.INVALID_BLOCK_TIMESTAMP_OLDER_THAN_PARENT",
              "block timestamp not greater than parent"),
          substring(
              "BlockException.INVALID_LOG_BLOOM", "failed to validate output of imported block"),
          substring(
              "BlockException.INVALID_RECEIPTS_ROOT", "failed to validate output of imported block"),
          substring(
              "BlockException.INVALID_STATE_ROOT", "World State Root does not match expected value"),
          regex(
              "BlockException.INVALID_REQUESTS",
              "Invalid execution requests|Requests hash mismatch, calculated: 0x[0-9a-f]+ header:"
                  + " 0x[0-9a-f]+"),
          regex(
              "BlockException.INVALID_BLOCK_HASH",
              "Computed block hash 0x[0-9a-f]+ does not match block hash parameter 0x[0-9a-f]+"),
          regex(
              "BlockException.SYSTEM_CONTRACT_CALL_FAILED",
              "System call halted|System call did not execute to completion"),
          regex(
              "BlockException.SYSTEM_CONTRACT_EMPTY",
              "(Invalid system call, no code at address)|(Invalid system call address:)"),
          regex(
              "BlockException.INVALID_DEPOSIT_EVENT_LAYOUT",
              "Invalid (amount|index|pubKey|signature|withdrawalCred) (offset|size): expected"
                  + " (\\d+), but got (-?\\d+)|Invalid deposit log length\\. Must be \\d+ bytes, but"
                  + " is \\d+ bytes"),
          regex(
              "BlockException.RLP_BLOCK_LIMIT_EXCEEDED",
              "Block size of \\d+ bytes exceeds limit of \\d+ bytes"),
          regex(
              "TransactionException.INITCODE_SIZE_EXCEEDED",
              "transaction invalid Initcode size of \\d+ exceeds maximum size of \\d+"),
          regex(
              "TransactionException.INSUFFICIENT_ACCOUNT_FUNDS",
              "transaction invalid transaction up-front cost 0x[0-9a-f]+ exceeds transaction sender"
                  + " account balance 0x[0-9a-f]+"),
          regex(
              "TransactionException.INTRINSIC_GAS_TOO_LOW",
              "transaction invalid intrinsic gas cost \\d+(?: \\(regular \\d+ \\+ state \\d+\\))?"
                  + " exceeds gas limit \\d+"),
          regex(
              "TransactionException.INTRINSIC_GAS_BELOW_FLOOR_GAS_COST",
              "transaction invalid intrinsic gas cost \\d+(?: \\(regular \\d+ \\+ state \\d+\\))?"
                  + " exceeds gas limit \\d+"),
          regex(
              "TransactionException.SENDER_NOT_EOA",
              "transaction invalid Sender 0x[0-9a-f]+ has deployed code and so is not authorized to"
                  + " send transactions"),
          regex(
              "TransactionException.NONCE_MISMATCH_TOO_LOW",
              "transaction invalid transaction nonce \\d+ below sender account nonce \\d+"),
          regex(
              "TransactionException.NONCE_MISMATCH_TOO_HIGH",
              "transaction invalid transaction nonce \\d+ does not match sender account nonce \\d+"),
          regex(
              "TransactionException.GAS_LIMIT_EXCEEDS_MAXIMUM",
              "transaction invalid Transaction gas limit must be at most \\d+"),
          regex(
              "TransactionException.TYPE_3_TX_MAX_BLOB_GAS_ALLOWANCE_EXCEEDED",
              "Blob transaction 0x[0-9a-f]+ exceeds block blob gas limit: \\d+ > \\d+"),
          regex(
              "TransactionException.TYPE_3_TX_BLOB_COUNT_EXCEEDED",
              "Blob transaction has too many blobs: \\d+|Invalid Blob Count: \\d+"),
          regex(
              "BlockException.INVALID_BAL_HASH",
              "Block access list hash mismatch, calculated:\\s*(0x[a-f0-9]+)\\s+header:\\s*"
                  + "(0x[a-f0-9]+)"),
          regex(
              "BlockException.BLOCK_ACCESS_LIST_GAS_LIMIT_EXCEEDED",
              "Block access list validation failed for block 0x[a-f0-9]+"),
          regex(
              "BlockException.INVALID_BLOCK_ACCESS_LIST",
              "Block access list hash mismatch, calculated:\\s*(0x[a-f0-9]+)\\s+header:\\s*"
                  + "(0x[a-f0-9]+)|Block access list validation failed for block 0x[a-f0-9]+"),
          regex(
              "BlockException.INCORRECT_BLOCK_FORMAT",
              "Block access list hash mismatch, calculated:\\s*(0x[a-f0-9]+)\\s+header:\\s*"
                  + "(0x[a-f0-9]+)|Block access list validation failed for block 0x[a-f0-9]+"));

  /**
   * Returns the set of EEST exception names whose Besu message pattern matches {@code message}.
   *
   * @param message the validation error message returned by Besu
   * @return the matching exception names (possibly empty if none match)
   */
  static Set<String> match(final String message) {
    final Set<String> matched = new LinkedHashSet<>();
    if (message == null) {
      return matched;
    }
    for (final Mapping mapping : MAPPINGS) {
      if (mapping.pattern().matcher(message).find()) {
        matched.add(mapping.exception());
      }
    }
    return matched;
  }

  /**
   * Checks an expected validation error (possibly a {@code |}-separated set of alternatives) against
   * the exceptions Besu's message maps to, mirroring hive's strict matching.
   *
   * @param expectedValidationError the fixture's expected validationError
   * @param besuMessage the message Besu returned with the INVALID status
   * @return {@code null} when the actual error matches one of the expected alternatives, otherwise a
   *     failure reason
   */
  static String mismatch(final String expectedValidationError, final String besuMessage) {
    final Set<String> actual = match(besuMessage);
    for (final String alternative : Splitter.on('|').trimResults().split(expectedValidationError)) {
      if (actual.contains(alternative)) {
        return null;
      }
    }
    return String.format(
        "expected validation error %s, but Besu returned %s (\"%s\")",
        expectedValidationError, actual.isEmpty() ? "an unmapped error" : actual, besuMessage);
  }
}
