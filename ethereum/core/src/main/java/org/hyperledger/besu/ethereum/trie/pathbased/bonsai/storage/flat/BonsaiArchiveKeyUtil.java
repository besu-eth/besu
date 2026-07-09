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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.bouncycastle.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared static utilities for building and reading archive-keyed entries. */
public class BonsaiArchiveKeyUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveKeyUtil.class);

  public static final byte[] MAX_BLOCK_SUFFIX =
      Bytes.ofUnsignedLong(Long.MAX_VALUE).toArrayUnsafe();
  public static final byte[] MIN_BLOCK_SUFFIX = Bytes.ofUnsignedLong(0L).toArrayUnsafe();
  public static final int KEY_SUFFIX_LENGTH = 8;

  private BonsaiArchiveKeyUtil() {}

  public static Optional<BonsaiContext> getStateArchiveContextForRead(
      final SegmentedKeyValueStorage storage) {
    // Memoized per proof (no-op outside a proof scope) so it isn't re-resolved from storage on
    // every flat-DB read.
    return BonsaiArchiveReadContext.flatReadContext(
        () -> {
          Optional<byte[]> archiveContext =
              storage.get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY);
          if (archiveContext.isPresent()) {
            try {
              return Optional.of(new BonsaiContext(Bytes.wrap(archiveContext.get()).toLong()));
            } catch (NumberFormatException e) {
              throw new IllegalStateException(
                  "World state archive context invalid format: "
                      + new String(archiveContext.get(), StandardCharsets.UTF_8));
            }
          }
          return Optional.empty();
        });
  }

  public static Bytes calculateArchiveKeyWithMaxSuffix(
      final Optional<BonsaiContext> context, final byte[] naturalKey) {
    return Bytes.of(calculateArchiveKeyWithSuffix(context, naturalKey, MAX_BLOCK_SUFFIX));
  }

  public static byte[] calculateArchiveKeyWithMinSuffix(
      final BonsaiContext context, final byte[] naturalKey) {
    return calculateArchiveKeyWithSuffix(Optional.of(context), naturalKey, MIN_BLOCK_SUFFIX);
  }

  public static byte[] calculateArchiveKeyNoContextMinSuffix(final byte[] naturalKey) {
    return Arrays.concatenate(naturalKey, MIN_BLOCK_SUFFIX);
  }

  public static byte[] calculateArchiveKeyNoContextMaxSuffix(final byte[] naturalKey) {
    return Arrays.concatenate(naturalKey, MAX_BLOCK_SUFFIX);
  }

  public static byte[] calculateArchiveKeyWithSuffix(
      final Optional<BonsaiContext> context, final byte[] naturalKey, final byte[] orElseSuffix) {
    return Arrays.concatenate(
        naturalKey,
        context
            .flatMap(BonsaiContext::getBlockNumber)
            .map(Bytes::ofUnsignedLong)
            .map(Bytes::toArrayUnsafe)
            .orElseGet(
                () -> {
                  LOG.atDebug().setMessage("Block context not present, using default suffix").log();
                  return orElseSuffix;
                }));
  }
}
