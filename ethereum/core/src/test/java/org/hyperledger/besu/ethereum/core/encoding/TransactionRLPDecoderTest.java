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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.evm.account.Account.MAX_NONCE;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.CodeDelegation;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TransactionRLPDecoderTest {

  private static final String FRONTIER_TX_RLP =
      "0xf901fc8032830138808080b901ae60056013565b6101918061001d6000396000f35b3360008190555056006001600060e060020a6000350480630a874df61461003a57806341c0e1b514610058578063a02b161e14610066578063dbbdf0831461007757005b610045600435610149565b80600160a060020a031660005260206000f35b610060610161565b60006000f35b6100716004356100d4565b60006000f35b61008560043560243561008b565b60006000f35b600054600160a060020a031632600160a060020a031614156100ac576100b1565b6100d0565b8060018360005260205260406000208190555081600060005260206000a15b5050565b600054600160a060020a031633600160a060020a031614158015610118575033600160a060020a0316600182600052602052604060002054600160a060020a031614155b61012157610126565b610146565b600060018260005260205260406000208190555080600060005260206000a15b50565b60006001826000526020526040600020549050919050565b600054600160a060020a031633600160a060020a0316146101815761018f565b600054600160a060020a0316ff5b561ca0c5689ed1ad124753d54576dfb4b571465a41900a1dff4058d8adf16f752013d0a01221cbd70ec28c94a3b55ec771bcbc70778d6ee0b51ca7ea9514594c861b1884";
  // NOTE: this is the canonical EIP-1559 opaque bytes wrapped one extra time as an RLP
  // byte-string (0xb8a9 <canonical 0x02f8a6... bytes>), not the canonical form itself.
  // decodeEIP1559NominalCase below decodes it via decodeRLP(RLP.input(...)), which strips
  // the wrapper; decodeOpaqueBytesShouldRejectRlpWrappedTypedTransaction decodes it via
  // decodeOpaqueBytes(..., BLOCK_BODY), which must reject that same wrapper. Two tests rely
  // on this constant for opposite reasons -- do not "fix" it to be unwrapped.
  private static final String EIP1559_TX_RLP =
      "0xb8a902f8a686796f6c6f7632800285012a05f20082753094000000000000000000000000000000000000aaaa8080f838f794000000000000000000000000000000000000aaaae1a0000000000000000000000000000000000000000000000000000000000000000001a00c1d69648e348fe26155b45de45004f0e4195f6352d8f0935bc93e98a3e2a862a060064e5b9765c0ac74223b0cf49635c59ae0faf82044fd17bcc68a549ade6f95";
  private static final String NONCE_64_BIT_MAX_MINUS_2_TX_RLP =
      "0xf86788fffffffffffffffe0182520894095e7baea6a6c7c4c2dfeb977efac326af552d8780801ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804";
  private static final String EIP7702_SET_CODE_TX_OPAQUE =
      "0x04f8c1018080078307a12094a94f5374fce5edbc8e2a8697c15331677e6ebf0b8080c0f85cf85a809400000000000000000000000000000000000010008080a0dbcff17ff6c249f13b334fa86bcbaa1afd9f566ca9b06e4ea5fab9bdde9a9202a05c34c9d8af5b20e4a425fc1daf2d9d484576857eaf1629145b4686bac733868e01a0d61673cd58ffa5fc605c3215aa4647fa3afbea1d1f577e08402442992526d980a0063068ca818025c7b8493d0623cb70ef3a2ba4b3e2ae0af1146d1c9b065c0aff";

  @Test
  void decodeEIP7702SetCodeTransaction() {
    final Transaction transaction =
        TransactionDecoder.decodeOpaqueBytes(
            Bytes.fromHexString(EIP7702_SET_CODE_TX_OPAQUE), EncodingContext.BLOCK_BODY);
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    TransactionEncoder.encodeRLP(transaction, rlpOut, EncodingContext.BLOCK_BODY);
    assertThat(rlpOut.encoded())
        .isEqualTo(
            Bytes.fromHexString(
                "0xb8c404f8c1018080078307a12094a94f5374fce5edbc8e2a8697c15331677e6ebf0b8080c0f85cf85a809400000000000000000000000000000000000010008080a0dbcff17ff6c249f13b334fa86bcbaa1afd9f566ca9b06e4ea5fab9bdde9a9202a05c34c9d8af5b20e4a425fc1daf2d9d484576857eaf1629145b4686bac733868e01a0d61673cd58ffa5fc605c3215aa4647fa3afbea1d1f577e08402442992526d980a0063068ca818025c7b8493d0623cb70ef3a2ba4b3e2ae0af1146d1c9b065c0aff"));
    assertThat(transaction.getType()).isEqualTo(TransactionType.DELEGATE_CODE);
    assertThat(transaction.getCodeDelegationList()).isPresent();
    assertThat(transaction.getCodeDelegationList().get().size()).isEqualTo(1);
    var expectedCodeDelegation =
        CodeDelegation.createCodeDelegation(
            BigInteger.ZERO,
            Address.fromHexString("0x1000"),
            "0x0",
            "0x0",
            "0xdbcff17ff6c249f13b334fa86bcbaa1afd9f566ca9b06e4ea5fab9bdde9a9202",
            "0x5c34c9d8af5b20e4a425fc1daf2d9d484576857eaf1629145b4686bac733868e");
    var actualCodeDelegation = transaction.getCodeDelegationList().get().getFirst();
    assertThat(actualCodeDelegation.chainId()).isEqualTo(expectedCodeDelegation.chainId());
    assertThat(actualCodeDelegation.address()).isEqualTo(expectedCodeDelegation.address());
    assertThat(actualCodeDelegation.nonce()).isEqualTo(expectedCodeDelegation.nonce());
    assertThat(actualCodeDelegation.signature()).isEqualTo(expectedCodeDelegation.signature());
  }

  @Test
  void decodeFrontierNominalCase() {
    final Transaction transaction = decodeRLP(RLP.input(Bytes.fromHexString(FRONTIER_TX_RLP)));
    assertThat(transaction).isNotNull();
    assertThat(transaction.getGasPrice().get()).isEqualByComparingTo(Wei.of(50L));
    assertThat(transaction.getMaxPriorityFeePerGas()).isEmpty();
    assertThat(transaction.getMaxFeePerGas()).isEmpty();
  }

  @Test
  void decodeEIP1559NominalCase() {
    final Transaction transaction = decodeRLP(RLP.input(Bytes.fromHexString(EIP1559_TX_RLP)));
    assertThat(transaction).isNotNull();
    assertThat(transaction.getMaxPriorityFeePerGas()).hasValue(Wei.of(2L));
    assertThat(transaction.getMaxFeePerGas()).hasValue(Wei.of(new BigInteger("5000000000", 10)));
  }

  @Test
  void doesNotDecodeEIP1559WithLargeMaxFeePerGasOrLargeMaxPriorityFeePerGas() {
    final String txWithBigFees =
        "0x02f84e0101a1648a5f8b2dcad5ea5ba6b720ff069c1d87c21a4a6a5b3766b39e2c2792367bb066a1ffa5ffaf5b0560d3a9fb186c2ede2ae6751bc0b4fef9107cf36389630b6196a38805800180c0010203";
    assertThatThrownBy(
            () ->
                TransactionDecoder.decodeOpaqueBytes(
                    Bytes.fromHexString(txWithBigFees), EncodingContext.BLOCK_BODY))
        .isInstanceOf(RLPException.class);
  }

  @Test
  void shouldDecodeWithHighNonce() {
    final Transaction transaction =
        decodeRLP(RLP.input(Bytes.fromHexString(NONCE_64_BIT_MAX_MINUS_2_TX_RLP)));
    assertThat(transaction).isNotNull();
    assertThat(transaction.getNonce()).isEqualTo(MAX_NONCE - 1);
  }

  @Test
  void shouldThrowIllegalArgWithEmptyBytes() {
    assertThatThrownBy(
            () -> TransactionDecoder.decodeOpaqueBytes(Bytes.EMPTY, EncodingContext.BLOCK_BODY))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decodeOpaqueBytesShouldRejectRlpWrappedTypedTransaction() {
    // EIP1559_TX_RLP is the canonical opaque bytes wrapped one extra time as an RLP byte-string
    // (0xb8a9 <canonical 0x02f8a6... bytes>). decodeOpaqueBytes has no surrounding RLP container
    // to legitimately unwrap, so it must reject this rather than stripping the wrapper and
    // decoding the inner transaction, which would otherwise let a block built from the wrapped
    // bytes hash identically to one built from the canonical bytes.
    assertThatThrownBy(
            () ->
                TransactionDecoder.decodeOpaqueBytes(
                    Bytes.fromHexString(EIP1559_TX_RLP), EncodingContext.BLOCK_BODY))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decodeOpaqueBytesShouldAcceptRlpWrappedTypedTransactionForPooledTransactionContext() {
    // Unlike BLOCK_BODY, POOLED_TRANSACTION has no canonical-hash consistency requirement to
    // protect, so decodeOpaqueBytes retains its pre-existing leniency here and falls back to
    // stripping the RLP wrapper -- the same behaviour decodeEIP1559NominalCase exercises
    // directly via decodeRLP(RLP.input(...)). This is the paired acceptance case for
    // decodeOpaqueBytesShouldRejectRlpWrappedTypedTransaction above.
    final Transaction transaction =
        TransactionDecoder.decodeOpaqueBytes(
            Bytes.fromHexString(EIP1559_TX_RLP), EncodingContext.POOLED_TRANSACTION);
    assertThat(transaction).isNotNull();
    assertThat(transaction.getMaxPriorityFeePerGas()).hasValue(Wei.of(2L));
    assertThat(transaction.getMaxFeePerGas()).hasValue(Wei.of(new BigInteger("5000000000", 10)));
  }

  @Test
  void testForAccessListTransaction() {
    BlockDataGenerator gen = new BlockDataGenerator();
    Transaction accessListTransaction = gen.transaction(TransactionType.ACCESS_LIST);
    Bytes encodedBytes =
        TransactionEncoder.encodeOpaqueBytes(accessListTransaction, EncodingContext.BLOCK_BODY);
    Transaction decodedTransaction =
        TransactionDecoder.decodeOpaqueBytes(encodedBytes, EncodingContext.BLOCK_BODY);
    assertThat(accessListTransaction).isEqualTo(decodedTransaction);
    Bytes reencodedBytes =
        TransactionEncoder.encodeOpaqueBytes(decodedTransaction, EncodingContext.BLOCK_BODY);
    assertThat(encodedBytes).isEqualTo(reencodedBytes);
  }

  private static Collection<Object[]> dataTransactionSize() {
    return Arrays.asList(
        new Object[][] {
          {FRONTIER_TX_RLP, "FRONTIER_TX_RLP", true},
          {EIP1559_TX_RLP, "EIP1559_TX_RLP", true},
          {NONCE_64_BIT_MAX_MINUS_2_TX_RLP, "NONCE_64_BIT_MAX_MINUS_2_TX_RLP", true},
          {
            "b89d01f89a0130308263309430303030303030303030303030303030303030303030f838f7943030303030303030303030303030303030303030e0a0303030303030303030303030303030303030303030303030303030303030303001a03130303130303031313031313031303130303030323030323030323030313030a03030303030303030303030303030303030303030303030303030303030303030",
            "too large for enclosing list",
            false
          },
          {
            "b84401f8410130308330303080308430303030d6d5943030303030303030303030303030303030303030c0808230309630303030303030303030303030303030303030303030",
            "list ends outside of enclosing list",
            false
          },
          {
            "9602d4013030308430303030803080c084303030013030",
            "Cannot read a unsigned byte scalar, expecting a maximum of 1 bytes but current element is 4 bytes long",
            false
          }
        });
  }

  @ParameterizedTest(name = "[{index}] {1}")
  @MethodSource("dataTransactionSize")
  void shouldCalculateCorrectTransactionSize(
      final String rlp_tx, final String ignoredName, final boolean valid) {
    assumeTrue(valid);
    // Create bytes from String
    final Bytes bytes = Bytes.fromHexString(rlp_tx);
    // Decode bytes into a transaction
    final Transaction transaction = decodeRLP(RLP.input(bytes));
    Bytes transactionBytes =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
    // Bytes size should be equal to transaction size
    assertThat(transaction.getSizeForAnnouncement()).isEqualTo(transactionBytes.size());
  }

  @ParameterizedTest(name = "[{index}] {1}")
  @MethodSource("dataTransactionSize")
  void shouldReturnCorrectEncodedBytes(
      final String txRlp, final String ignoredName, final boolean valid) {
    assumeTrue(valid);
    final Transaction transaction = decodeRLP(RLP.input(Bytes.fromHexString(txRlp)));
    assertThat(transaction.encoded()).isEqualTo(Bytes.fromHexString(txRlp));
  }

  @ParameterizedTest(name = "[{index}] {1}")
  @MethodSource("dataTransactionSize")
  void shouldDecodeRLP(final String txRlp, final String name, final boolean valid) {
    if (valid) {
      // thrown exceptions will break test
      decodeRLP(RLP.input(Bytes.fromHexString(txRlp)));
    } else {
      assertThatThrownBy(() -> decodeRLP(RLP.input(Bytes.fromHexString(txRlp))))
          .isInstanceOf(RLPException.class)
          .hasMessageContaining(name);
    }
  }

  private Transaction decodeRLP(final RLPInput input) {
    return TransactionDecoder.decodeRLP(input, EncodingContext.POOLED_TRANSACTION);
  }
}
