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
package org.hyperledger.besu.consensus.common.bft.payload;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes32;

/** Signatures a crafted BFT message can carry that decode but do not recover to an address. */
public final class NonRecoverableSignature {

  private NonRecoverableSignature() {}

  /**
   * A structurally valid signature (r and s in range, s in the lower half, so it decodes and passes
   * the high-S check) whose r is not the x-coordinate of a curve point, so public key recovery
   * fails whatever the signed hash. Found by probe so it holds for both the native and BouncyCastle
   * recovery backends.
   *
   * @return the signature
   */
  public static SECPSignature create() {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final Bytes32 anyHash = Bytes32.repeat((byte) 0x11);
    for (long r = 1; r < 1000; r++) {
      final SECPSignature candidate =
          new SECPSignature(BigInteger.valueOf(r), BigInteger.ONE, (byte) 0);
      if (signatureAlgorithm.recoverPublicKeyFromSignature(anyHash, candidate).isEmpty()) {
        return candidate;
      }
    }
    throw new IllegalStateException("expected a non-recoverable r within the probe range");
  }

  /**
   * Write a payload the way {@link SignedData#writeTo(RLPOutput)} does, but with a signature that
   * does not recover, as a peer would when swapping it into an otherwise genuine message.
   *
   * @param payload the payload
   * @param output the output
   */
  public static void writeSigned(final Payload payload, final RLPOutput output) {
    output.startList();
    payload.writeTo(output);
    output.writeBytes(create().encodedBytes());
    output.endList();
  }
}
