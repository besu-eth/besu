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
package org.hyperledger.besu.ethereum.trie;

import org.bouncycastle.crypto.digests.KeccakDigest;

/** Keccak-256 over byte arrays, with one reusable digest per thread. */
public final class Keccak256 {

  /** Size of a hash in bytes. */
  public static final int SIZE = 32;

  private static final ThreadLocal<KeccakDigest> DIGEST =
      ThreadLocal.withInitial(() -> new KeccakDigest(256));

  private Keccak256() {}

  public static byte[] hash(final byte[] input) {
    final KeccakDigest digest = DIGEST.get();
    final byte[] out = new byte[SIZE];
    try {
      digest.update(input, 0, input.length);
      digest.doFinal(out, 0);
    } catch (final RuntimeException e) {
      // never leave a half-updated digest behind for the next caller of this thread
      digest.reset();
      throw e;
    }
    return out;
  }
}
