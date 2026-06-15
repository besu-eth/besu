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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class CodeDelegationDecoderTest {

  @Test
  void shouldDecodeInnerPayloadWithNonce() {
    // "0xd80194633688abc3ccf8b0c03088d2d1c6ae4958c2fa56c105"

    final BytesValueRLPInput input =
        new BytesValueRLPInput(
            Bytes.fromHexString(
                "0xf85a0194633688abc3ccf8b0c03088d2d1c6ae4958c2fa562a80a0840798fa67118e034c1eb7e42fe89e28d7cd5006dc813d5729e5f75b0d1a7ec5a03b1dbace38ceb862a65bf2eac0637693b5c3493bcb2a022dd614c0a74cce0b99"),
            true);
    final CodeDelegation codeDelegation =
        CodeDelegationTransactionDecoder.decodeInnerPayload(input);

    assertThat(codeDelegation.chainId()).isEqualTo(BigInteger.ONE);
    assertThat(codeDelegation.address())
        .isEqualTo(Address.fromHexStringStrict("0x633688abc3cCf8B0C03088D2d1C6ae4958c2fA56"));
    assertThat(codeDelegation.nonce()).isEqualTo(42);

    final SECPSignature signature = codeDelegation.signature();
    assertThat(signature.getRecId()).isEqualTo((byte) 0);
    assertThat(signature.getR().toString(16))
        .isEqualTo("840798fa67118e034c1eb7e42fe89e28d7cd5006dc813d5729e5f75b0d1a7ec5");
    assertThat(signature.getS().toString(16))
        .isEqualTo("3b1dbace38ceb862a65bf2eac0637693b5c3493bcb2a022dd614c0a74cce0b99");
  }

  @Test
  void shouldDecodeInnerPayloadWithNonceZero() {
    // "0xd70194633688abc3ccf8b0c03088d2d1c6ae4958c2fa56c5"

    final BytesValueRLPInput input =
        new BytesValueRLPInput(
            Bytes.fromHexString(
                "0xf85a0194633688abc3ccf8b0c03088d2d1c6ae4958c2fa568001a0dd6b24048be1b7d7fe5bbbb73ffc37eb2ce1997ecb4ae5b6096532ef19363148a025b58a1ff8ad00bddbbfa1d5c2411961cbb6d08dcdc8ae88303db3c6cf983031"),
            true);
    final CodeDelegation codeDelegation =
        CodeDelegationTransactionDecoder.decodeInnerPayload(input);

    assertThat(codeDelegation.chainId()).isEqualTo(BigInteger.ONE);
    assertThat(codeDelegation.address())
        .isEqualTo(Address.fromHexStringStrict("0x633688abc3cCf8B0C03088D2d1C6ae4958c2fA56"));
    assertThat(codeDelegation.nonce()).isEqualTo(0);

    final SECPSignature signature = codeDelegation.signature();
    assertThat(signature.getRecId()).isEqualTo((byte) 1);
    assertThat(signature.getR().toString(16))
        .isEqualTo("dd6b24048be1b7d7fe5bbbb73ffc37eb2ce1997ecb4ae5b6096532ef19363148");
    assertThat(signature.getS().toString(16))
        .isEqualTo("25b58a1ff8ad00bddbbfa1d5c2411961cbb6d08dcdc8ae88303db3c6cf983031");
  }

  @Test
  void shouldDecodeInnerPayloadWithChainIdZero() {
    // "d70094633688abc3ccf8b0c03088d2d1c6ae4958c2fa56c5"

    final BytesValueRLPInput input =
        new BytesValueRLPInput(
            Bytes.fromHexString(
                "0xf85a8094633688abc3ccf8b0c03088d2d1c6ae4958c2fa560501a0025c1240d7ffec0daeedb752d3357aff2e3cd58468f0c2d43ee0ee999e02ace2a03c8a25b2becd6e666f69803d1ae3322f2e137b7745c2c7f19da80f993ffde4df"),
            true);
    final CodeDelegation codeDelegation =
        CodeDelegationTransactionDecoder.decodeInnerPayload(input);

    assertThat(codeDelegation.chainId()).isEqualTo(BigInteger.ZERO);
    assertThat(codeDelegation.address())
        .isEqualTo(Address.fromHexStringStrict("0x633688abc3cCf8B0C03088D2d1C6ae4958c2fA56"));
    assertThat(codeDelegation.nonce()).isEqualTo(5);

    final SECPSignature signature = codeDelegation.signature();
    assertThat(signature.getRecId()).isEqualTo((byte) 1);
    assertThat(signature.getR().toString(16))
        .isEqualTo("25c1240d7ffec0daeedb752d3357aff2e3cd58468f0c2d43ee0ee999e02ace2");
    assertThat(signature.getS().toString(16))
        .isEqualTo("3c8a25b2becd6e666f69803d1ae3322f2e137b7745c2c7f19da80f993ffde4df");
  }

  @Test
  void shouldDecodeInnerPayloadWhenSignatureIsZero() {
    final BytesValueRLPInput input =
        new BytesValueRLPInput(
            Bytes.fromHexString(
                "0xdf8501a1f0ff5a947a40026a3b9a41754a95eec8c92c6b99886f440c5b808080"),
            true);
    final CodeDelegation codeDelegation =
        CodeDelegationTransactionDecoder.decodeInnerPayload(input);

    assertThat(codeDelegation.chainId()).isEqualTo(new BigInteger("01a1f0ff5a", 16));
  }

  @Test
  void shouldDecodeInnerPayloadWithMaxUInt256ChainId() {
    final BytesValueRLPInput input =
        new BytesValueRLPInput(
            encodedCodeDelegation(BigInteger.TWO.pow(256).subtract(BigInteger.ONE)), true);

    final CodeDelegation codeDelegation =
        CodeDelegationTransactionDecoder.decodeInnerPayload(input);

    assertThat(codeDelegation.chainId())
        .isEqualTo(BigInteger.TWO.pow(256).subtract(BigInteger.ONE));
  }

  @Test
  void shouldRejectInnerPayloadWhenChainIdExceedsUInt256() {
    final BytesValueRLPInput input =
        new BytesValueRLPInput(encodedCodeDelegation(BigInteger.TWO.pow(256)), true);

    assertThatThrownBy(() -> CodeDelegationTransactionDecoder.decodeInnerPayload(input))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("32-bytes scalar");
  }

  private Bytes encodedCodeDelegation(final BigInteger chainId) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    output.startList();
    output.writeBigIntegerScalar(chainId);
    output.writeBytes(Address.ZERO.getBytes().copy());
    output.writeLongScalar(0);
    output.writeUnsignedByte(0);
    output.writeBigIntegerScalar(BigInteger.ONE);
    output.writeBigIntegerScalar(BigInteger.ONE);
    output.endList();
    return output.encoded();
  }
}
