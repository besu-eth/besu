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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.crypto.CodeDelegationSignature;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class CodeDelegationTest {

  // secp256k1 curve order (n)
  private static final BigInteger SECP256K1_N =
      new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16);

  private BigInteger chainId;
  private Address address;
  private long nonce;
  private SECPSignature signature;
  private SignatureAlgorithm signatureAlgorithm;

  @BeforeEach
  void setUp() {
    chainId = BigInteger.valueOf(1);
    address = Address.fromHexString("0x1234567890abcdef1234567890abcdef12345678");
    nonce = 100;

    signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    KeyPair keyPair = signatureAlgorithm.generateKeyPair();
    signature = signatureAlgorithm.sign(Bytes32.fromHexStringLenient("deadbeef"), keyPair);
  }

  @Test
  void shouldCreateCodeDelegationSuccessfully() {
    CodeDelegation delegation = new CodeDelegation(chainId, address, nonce, signature);

    assertThat(delegation.chainId()).isEqualTo(chainId);
    assertThat(delegation.address()).isEqualTo(address);
    assertThat(delegation.nonce()).isEqualTo(nonce);
    assertThat(delegation.signature()).isEqualTo(signature);
  }

  @Test
  void shouldBuildCodeDelegationWithBuilder() {
    CodeDelegation delegation =
        (CodeDelegation)
            CodeDelegation.builder()
                .chainId(chainId)
                .address(address)
                .nonce(nonce)
                .signature(signature)
                .build();

    assertThat(delegation).isNotNull();
    assertThat(delegation.chainId()).isEqualTo(chainId);
    assertThat(delegation.address()).isEqualTo(address);
    assertThat(delegation.nonce()).isEqualTo(nonce);
    assertThat(delegation.signature()).isEqualTo(signature);
  }

  @Test
  void shouldThrowWhenBuildingWithoutAddress() {
    assertThatThrownBy(
            () ->
                CodeDelegation.builder().chainId(chainId).nonce(nonce).signature(signature).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Address must be set");
  }

  @Test
  void shouldThrowWhenBuildingWithoutNonce() {
    assertThatThrownBy(
            () ->
                CodeDelegation.builder()
                    .chainId(chainId)
                    .address(address)
                    .signature(signature)
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Nonce must be set");
  }

  @Test
  void shouldThrowWhenBuildingWithoutSignature() {
    assertThatThrownBy(
            () -> CodeDelegation.builder().chainId(chainId).address(address).nonce(nonce).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Signature must be set");
  }

  @Test
  void shouldCreateCodeDelegationUsingFactoryMethod() {
    CodeDelegation delegation =
        (CodeDelegation)
            CodeDelegation.createCodeDelegation(
                chainId, address, "0x64", "0x1b", "0xabcdef", "0x123456");

    assertThat(delegation).isNotNull();
    assertThat(delegation.chainId()).isEqualTo(chainId);
    assertThat(delegation.address()).isEqualTo(address);
    assertThat(delegation.nonce()).isEqualTo(100);
  }

  @Test
  void shouldReturnAuthorizerWhenSignatureIsValid() {
    CodeDelegation delegation = new CodeDelegation(chainId, address, nonce, signature);

    Optional<Address> authorizer = delegation.authorizer();

    assertThat(authorizer).isNotEmpty();
  }

  @Test
  void shouldReturnEmptyAuthorizerWhenSignatureInvalid() {
    SECPSignature invalidSignature = Mockito.mock(SECPSignature.class);
    Mockito.when(invalidSignature.getRecId()).thenReturn((byte) 5); // Invalid recId (>3)

    CodeDelegation delegation = new CodeDelegation(chainId, address, nonce, invalidSignature);

    Optional<Address> authorizer = delegation.authorizer();

    assertThat(authorizer).isEmpty();
  }

  @Test
  void shouldReturnEmptyAuthorizerWhenREqualsSecp256k1N() {
    CodeDelegationSignature badSig =
        CodeDelegationSignature.create(SECP256K1_N, BigInteger.ONE, (byte) 0);

    CodeDelegation delegation = new CodeDelegation(chainId, address, nonce, badSig);

    assertThat(delegation.authorizer()).isEmpty();
  }

  @Test
  void shouldReturnEmptyAuthorizerWhenRIsZero() {
    CodeDelegationSignature badSig =
        CodeDelegationSignature.create(BigInteger.ZERO, BigInteger.ONE, (byte) 0);

    CodeDelegation delegation = new CodeDelegation(chainId, address, nonce, badSig);

    assertThat(delegation.authorizer()).isEmpty();
  }

  @Test
  void shouldReturnCorrectSignatureValues() {
    CodeDelegation delegation = new CodeDelegation(chainId, address, nonce, signature);

    assertThat(delegation.v()).isEqualTo(signature.getRecId());
    assertThat(delegation.r()).isEqualTo(signature.getR());
    assertThat(delegation.s()).isEqualTo(signature.getS());
  }

  @Test
  void shouldSignAndBuildUsingKeyPair() {
    KeyPair keyPair = signatureAlgorithm.generateKeyPair();

    CodeDelegation delegation =
        (CodeDelegation)
            CodeDelegation.builder()
                .chainId(chainId)
                .address(address)
                .nonce(nonce)
                .signAndBuild(keyPair);

    assertThat(delegation).isNotNull();
    assertThat(delegation.signature()).isNotNull();
  }
}
