/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.cryptoservices;

import org.hyperledger.besu.crypto.ECPointUtil;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModule;
import org.hyperledger.besu.plugin.services.securitymodule.data.PublicKey;
import org.hyperledger.besu.plugin.services.securitymodule.data.Signature;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** The Node key. */
public class NodeKey {

  private final SecurityModule securityModule;
  private final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();

  /**
   * Instantiates a new Node key.
   *
   * @param securityModule the security module
   */
  public NodeKey(final SecurityModule securityModule) {
    this.securityModule = securityModule;
  }

  /**
   * Sign hash and get secp signature.
   *
   * @param dataHash the data hash
   * @return the secp signature
   */
  public SECPSignature sign(final Bytes32 dataHash) {
    final Signature signature = securityModule.sign(dataHash);

    return signatureAlgorithm.normaliseSignature(
        signature.getR(), signature.getS(), getPublicKey(), dataHash);
  }

  /**
   * Gets public key.
   *
   * @return the public key
   */
  public SECPPublicKey getPublicKey() {
    return signatureAlgorithm.createPublicKey(
        ECPointUtil.getEncodedBytes(securityModule.getPublicKey().getW()));
  }

  /**
   * Calculate ecdh key agreement.
   *
   * @param partyKey the party key
   * @return the bytes32
   */
  public Bytes32 calculateECDHKeyAgreement(final SECPPublicKey partyKey) {
    return securityModule.calculateECDHKeyAgreement(asPluginPublicKey(partyKey));
  }

  /**
   * Calculate ECDH key agreement returning the compressed EC point.
   *
   * @param partyKey the party key
   * @return the compressed EC point (33 bytes)
   */
  public Bytes calculateECDHKeyAgreementCompressed(final SECPPublicKey partyKey) {
    return securityModule.calculateECDHKeyAgreementCompressed(asPluginPublicKey(partyKey));
  }

  private PublicKey asPluginPublicKey(final SECPPublicKey partyKey) {
    return () ->
        ECPointUtil.fromBouncyCastleECPoint(signatureAlgorithm.publicKeyAsEcPoint(partyKey));
  }
}
