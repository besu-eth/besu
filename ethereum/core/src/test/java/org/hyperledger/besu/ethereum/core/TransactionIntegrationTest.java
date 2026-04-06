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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class TransactionIntegrationTest {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair senderKeys = SIGNATURE_ALGORITHM.generateKeyPair();

  @Test
  public void
      shouldDecodeInterestingRlpFromARealTransactionGeneratedUsingRemixAndASimpleContract() {
    final String encodedString =
        "0xf902560c843b9aca00832dc6c08080b90202608060405234801561001057600080fd5b506040516020806101e283398101604052516000556101ae806100346000396000f30060806040526004361061006c5763ffffffff7c01000000000000000000000000000000000000000000000000000000006000350416632113522a81146100715780632a1afcd9146100af5780633bc5de30146100d657806360fe47b1146100eb578063db613e8114610105575b600080fd5b34801561007d57600080fd5b5061008661011a565b6040805173ffffffffffffffffffffffffffffffffffffffff9092168252519081900360200190f35b3480156100bb57600080fd5b506100c4610136565b60408051918252519081900360200190f35b3480156100e257600080fd5b506100c461013c565b3480156100f757600080fd5b50610103600435610142565b005b34801561011157600080fd5b50610086610166565b60015473ffffffffffffffffffffffffffffffffffffffff1681565b60005481565b60005490565b6000556001805473ffffffffffffffffffffffffffffffffffffffff191633179055565b60015473ffffffffffffffffffffffffffffffffffffffff16905600a165627a7a723058208293fac83e9cc01039adf5e41eefd557d1324a3a4c830a4802fa1dd2515227a20029000000000000000000000000000000000000000000000000000000000000007b820fe8a009e7b69af4c318e7fb915f53649bd9f99e5423d41c2cd6a01ab69bb34a951b2fa01b5d39b7c9041ec022d13e6e89eec2cddbe27572eda7956ada5de1032cd5da15";
    final Bytes encoded = Bytes.fromHexString(encodedString);
    final RLPInput input = RLP.input(encoded);
    final Transaction transaction = Transaction.readFrom(input);
    assertThat(transaction).isNotNull();
    assertThat(transaction.isContractCreation()).isTrue();
    assertThat(transaction.getChainId()).contains(BigInteger.valueOf(2018));
    assertThat(transaction.getSender())
        .isEqualTo(Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
  }

  @Test
  public void shouldDecodeAndEncodeTransactionCorrectly() {
    final String encodedString =
        "0xf9025780843b9aca00832dc6c08080b90202608060405234801561001057600080fd5b506040516020806101e283398101604052516000556101ae806100346000396000f30060806040526004361061006c5763ffffffff7c01000000000000000000000000000000000000000000000000000000006000350416632113522a81146100715780632a1afcd9146100af5780633bc5de30146100d657806360fe47b1146100eb578063db613e8114610105575b600080fd5b34801561007d57600080fd5b5061008661011a565b6040805173ffffffffffffffffffffffffffffffffffffffff9092168252519081900360200190f35b3480156100bb57600080fd5b506100c4610136565b60408051918252519081900360200190f35b3480156100e257600080fd5b506100c461013c565b3480156100f757600080fd5b50610103600435610142565b005b34801561011157600080fd5b50610086610166565b60015473ffffffffffffffffffffffffffffffffffffffff1681565b60005481565b60005490565b6000556001805473ffffffffffffffffffffffffffffffffffffffff191633179055565b60015473ffffffffffffffffffffffffffffffffffffffff16905600a165627a7a723058208293fac83e9cc01039adf5e41eefd557d1324a3a4c830a4802fa1dd2515227a20029000000000000000000000000000000000000000000000000000000000000000c830628cba0482ba9b1136cd9337408938eea6b991fd153900a014867da2f4bb113d4003888a00c5a2f8f279fe2c86831afb5c9578dd1c3be457e3aca3abe439b1a5dd122e676";
    final Bytes encoded = Bytes.fromHexString(encodedString);
    final RLPInput input = RLP.input(encoded);
    final Transaction transaction = Transaction.readFrom(input);
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    transaction.writeTo(output);
    assertThat(output.encoded().toString()).isEqualTo(encodedString);
  }

  @Test
  public void shouldDecodeTransactionWithLargeChainId() {
    final String encodedString =
        "0xf86a018609184e72a0008276c094d30c3d13b07029deba00de1da369cd69a02c20560180850100000021a07d344f26d7329e8932d2878b99f07b12752bbd13a0b3b822644dbf9600fe718da01d6e6b6c66e1aadf4e33e318a7eef03d3bd3602de52662f0cb5af5b372d44dcd";
    final Bytes encoded = Bytes.fromHexString(encodedString);
    final RLPInput input = RLP.input(encoded);
    final Transaction transaction = Transaction.readFrom(input);
    assertThat(transaction).isNotNull();
    assertThat(transaction.isContractCreation()).isFalse();
    assertThat(transaction.getChainId()).contains(BigInteger.valueOf(2147483647));
    assertThat(transaction.getSender())
        .isEqualTo(Address.fromHexString("0xdf664d0d2270ef97b48f222e3187bd14c8ca9428"));
    assertThat(transaction.getTo())
        .contains(Address.fromHexString("0xd30c3d13b07029deba00de1da369cd69a02c2056"));
  }

  @Test
  public void shouldReturnEncodedPreimageCorrectly() {
    final var tx =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .chainId(Optional.of(BigInteger.ONE))
            .nonce(2)
            .gasLimit(23456)
            .to(Optional.of(Address.fromHexString("0x1234000000000000000000000000000000004321")))
            .value(Wei.of(2048))
            .payload(Bytes.fromHexString("0xabcd"))
            .maxFeePerGas(Optional.of(Wei.of(12345)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(6789)))
            .createTransaction(senderKeys);

    final var encodedPreimage = tx.encodedPreimage();

    final var expectedEncodedPreimage =
        "0x02e70102821a85823039825ba094123400000000000000000000000000000000432182080082abcdc0";

    assertThat(encodedPreimage.toString()).isEqualTo(expectedEncodedPreimage);
  }
}
