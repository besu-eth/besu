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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Address;

import org.apache.tuweni.bytes.Bytes;

/**
 * A signature entry of an EIP-8141 frame transaction.
 *
 * @param scheme the verification scheme ({@link #SCHEME_ARBITRARY}, {@link #SCHEME_SECP256K1} or
 *     {@link #SCHEME_P256})
 * @param signer scheme-dependent signer metadata: empty, or a 20-byte address for SECP256K1/P256
 * @param msg empty for the canonical transaction signature hash, or an explicit 32-byte digest
 * @param signature raw signature bytes interpreted according to the scheme
 */
public record FrameSignature(int scheme, Bytes signer, Bytes msg, Bytes signature) {

  /** Arbitrary bytes, validated structurally only. */
  public static final int SCHEME_ARBITRARY = 0x0;

  /** secp256k1 signature encoded as {@code v (1 byte) || r (32 bytes) || s (32 bytes)}. */
  public static final int SCHEME_SECP256K1 = 0x1;

  /** P-256 signature encoded as {@code r || s || qx || qy} (32 bytes each). */
  public static final int SCHEME_P256 = 0x2;

  /**
   * Whether the entry uses the canonical transaction signature hash as its message.
   *
   * @return true when the msg field is empty
   */
  public boolean signsCanonicalHash() {
    return msg.isEmpty();
  }

  /**
   * The signer resolved against the transaction sender. Only meaningful for protocol-validated
   * schemes; ARBITRARY entries have no resolved signer.
   *
   * @param sender the transaction sender
   * @return the declared signer address, or the sender when the signer field is empty
   */
  public Address resolvedSigner(final Address sender) {
    return signer.isEmpty() ? sender : Address.wrap(signer);
  }
}
