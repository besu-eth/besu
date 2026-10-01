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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

/**
 * A staged change to one key: a put, a remove, or a merge that computes the new value from the
 * existing one during the update's descent.
 *
 * @param path the full nibble path of the key, ending with the leaf terminator
 * @param value the new value of a put, null otherwise
 * @param merger the function of a merge, null otherwise
 */
public record TreeUpdate(
    Bytes path, Bytes value, Function<Optional<Bytes>, Optional<Bytes>> merger) {

  /**
   * A put.
   *
   * @param path the key path
   * @param value the value
   * @return the update
   */
  public static TreeUpdate put(final Bytes path, final Bytes value) {
    return new TreeUpdate(path, value, null);
  }

  /**
   * A remove.
   *
   * @param path the key path
   * @return the update
   */
  public static TreeUpdate remove(final Bytes path) {
    return new TreeUpdate(path, null, null);
  }

  /**
   * A merge: {@code merger} receives the existing value and returns the new one, or empty to remove
   * the key. It is called exactly once.
   *
   * @param path the key path
   * @param merger the merge function
   * @return the update
   */
  public static TreeUpdate merge(
      final Bytes path, final Function<Optional<Bytes>, Optional<Bytes>> merger) {
    return new TreeUpdate(path, null, merger);
  }

  /**
   * Applies this update alone to the subtree at {@code node}, which sits at depth {@code offset}.
   */
  TreeNode applyTo(final TreeSession session, final TreeNode node, final int offset) {
    if (merger != null) {
      return TreeOps.merge(session, node, path, offset, merger);
    }
    if (value != null) {
      return TreeOps.put(session, node, path, offset, value);
    }
    return TreeOps.remove(session, node, path, offset);
  }

  /** The value of the key after this update, given its value before (null when absent). */
  Bytes applyToValue(final Bytes prior) {
    if (merger != null) {
      return merger.apply(Optional.ofNullable(prior)).orElse(null);
    }
    return value;
  }
}
