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
package org.hyperledger.besu.datatypes;

import org.apache.tuweni.bytes.Bytes;

/**
 * Represents a pre-validated signature within an EIP-8141 Frame Transaction. Signatures are
 * validated before any frames execute.
 *
 * @param scheme the signature verification algorithm (0=SECP256K1, 1=P256)
 * @param signer scheme-dependent signer metadata (20-byte address)
 * @param msg either empty (canonical tx hash) or explicit 32-byte digest
 * @param signature raw signature bytes
 */
public record FrameTransactionSignature(int scheme, Address signer, Bytes msg, Bytes signature) {

  /** SECP256K1 signature scheme. */
  public static final int SCHEME_SECP256K1 = 0;

  /** P256 signature scheme. */
  public static final int SCHEME_P256 = 1;

  /** Gas cost for SECP256K1 signature verification. */
  public static final long SECP256K1_GAS_COST = 2800L;

  /** Gas cost for P256 signature verification. */
  public static final long P256_GAS_COST = 6700L;

  /**
   * Returns the gas cost for verifying this signature.
   *
   * @return the verification gas cost
   */
  public long verificationGasCost() {
    return switch (scheme) {
      case SCHEME_SECP256K1 -> SECP256K1_GAS_COST;
      case SCHEME_P256 -> P256_GAS_COST;
      default -> throw new IllegalArgumentException("Unknown signature scheme: " + scheme);
    };
  }

  /**
   * Returns whether this signature uses the canonical transaction hash (empty msg).
   *
   * @return true if msg is empty
   */
  public boolean isCanonical() {
    return msg.isEmpty();
  }
}
