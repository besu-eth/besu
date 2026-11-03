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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static org.hyperledger.besu.datatypes.TransactionType.FRONTIER;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.ChainId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class SimpleTestTransactionBuilder {

  private static final int HEX_RADIX = 16;

  public static Transaction transaction(final Hash hash) {
    return transaction(
        hash,
        "0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b",
        "0x2fefd8",
        "0x1",
        "0x5b5b610705806100106000396000f3006000357c010000000000000000000000000000000000000000000000000000000090048063102accc11461012c57806312a7b9141461013a5780631774e6461461014c5780631e26fd331461015d5780631f9030371461016e578063343a875d1461018057806338cc4831146101955780634e7ad367146101bd57806357cb2fc4146101cb57806365538c73146101e057806368895979146101ee57806376bc21d9146102005780639a19a9531461020e5780639dc2c8f51461021f578063a53b1c1e1461022d578063a67808571461023e578063b61c05031461024c578063c2b12a731461025a578063d2282dc51461026b578063e30081a01461027c578063e8beef5b1461028d578063f38b06001461029b578063f5b53e17146102a9578063fd408767146102bb57005b6101346104d6565b60006000f35b61014261039b565b8060005260206000f35b610157600435610326565b60006000f35b6101686004356102c9565b60006000f35b610176610442565b8060005260206000f35b6101886103d3565b8060ff1660005260206000f35b61019d610413565b8073ffffffffffffffffffffffffffffffffffffffff1660005260206000f35b6101c56104c5565b60006000f35b6101d36103b7565b8060000b60005260206000f35b6101e8610454565b60006000f35b6101f6610401565b8060005260206000f35b61020861051f565b60006000f35b6102196004356102e5565b60006000f35b610227610693565b60006000f35b610238600435610342565b60006000f35b610246610484565b60006000f35b610254610493565b60006000f35b61026560043561038d565b60006000f35b610276600435610350565b60006000f35b61028760043561035e565b60006000f35b6102956105b4565b60006000f35b6102a3610547565b60006000f35b6102b16103ef565b8060005260206000f35b6102c3610600565b60006000f35b80600060006101000a81548160ff021916908302179055505b50565b80600060016101000a81548160ff02191690837f01000000000000000000000000000000000000000000000000000000000000009081020402179055505b50565b80600060026101000a81548160ff021916908302179055505b50565b806001600050819055505b50565b806002600050819055505b50565b80600360006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908302179055505b50565b806004600050819055505b50565b6000600060009054906101000a900460ff1690506103b4565b90565b6000600060019054906101000a900460000b90506103d0565b90565b6000600060029054906101000a900460ff1690506103ec565b90565b600060016000505490506103fe565b90565b60006002600050549050610410565b90565b6000600360009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16905061043f565b90565b60006004600050549050610451565b90565b7f65c9ac8011e286e89d02a269890f41d67ca2cc597b2c76c7c69321ff492be5806000602a81526020016000a15b565b6000602a81526020016000a05b565b60017f81933b308056e7e85668661dcd102b1f22795b4431f9cf4625794f381c271c6b6000602a81526020016000a25b565b60016000602a81526020016000a15b565b3373ffffffffffffffffffffffffffffffffffffffff1660017f0e216b62efbb97e751a2ce09f607048751720397ecfb9eef1e48a6644948985b6000602a81526020016000a35b565b3373ffffffffffffffffffffffffffffffffffffffff1660016000602a81526020016000a25b565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6001023373ffffffffffffffffffffffffffffffffffffffff1660017f317b31292193c2a4f561cc40a95ea0d97a2733f14af6d6d59522473e1f3ae65f6000602a81526020016000a45b565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6001023373ffffffffffffffffffffffffffffffffffffffff1660016000602a81526020016000a35b565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6001023373ffffffffffffffffffffffffffffffffffffffff1660017fd5f0a30e4be0c6be577a71eceb7464245a796a7e6a55c0d971837b250de05f4e60007fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe98152602001602a81526020016000a45b565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6001023373ffffffffffffffffffffffffffffffffffffffff16600160007fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe98152602001602a81526020016000a35b56",
        "0x0",
        null,
        "0xf8",
        "0xa",
        "0x1c",
        "0xe439aa8812c1c0a751b0931ea20c5a30cd54fe15cae883c59fd8107e04557679",
        "0x58d025af99b538b778a47da8115c43d5cee564c3cc8d58eb972aaf80ea2c406e");
  }

  public static Transaction transaction(
      final Hash blockHash,
      final String fromAddress,
      final String gas,
      final String gasPrice,
      final String input,
      final String nonce,
      final String toAddress,
      final String type,
      final String value,
      final String v,
      final String r,
      final String s) {

    final Transaction transaction = mock(Transaction.class);
    when(transaction.getHash()).thenReturn(blockHash);
    when(transaction.getGasPrice()).thenReturn(Optional.of(Wei.fromHexString(gasPrice)));
    when(transaction.getNonce()).thenReturn(unsignedLong(nonce));
    when(transaction.getV()).thenReturn(bigInteger(v));
    when(transaction.getR()).thenReturn(bigInteger(r));
    when(transaction.getS()).thenReturn(bigInteger(s));
    when(transaction.getTo()).thenReturn(Optional.ofNullable(address(toAddress)));
    when(transaction.getType())
        .thenReturn(
            TransactionType.fromOpaque(Bytes.fromHexString(type, 1).get(0)).orElse(FRONTIER));
    when(transaction.getSender()).thenReturn(address(fromAddress));
    when(transaction.getPayload()).thenReturn(Bytes.fromHexString(input));
    when(transaction.getValue()).thenReturn(wei(value));
    when(transaction.getGasLimit()).thenReturn(unsignedLong(gas));
    lenient().when(transaction.getChainIdentifier()).thenReturn(ChainId.absent());
    return transaction;
  }

  private static long unsignedLong(final String value) {
    final String hex = removeHexPrefix(value);
    return new BigInteger(hex, HEX_RADIX).longValue();
  }

  private static String removeHexPrefix(final String prefixedHex) {
    return prefixedHex.startsWith("0x") ? prefixedHex.substring(2) : prefixedHex;
  }

  private static BigInteger bigInteger(final String hex) {
    return new BigInteger(removeHexPrefix(hex), HEX_RADIX);
  }

  private static Wei wei(final String hex) {
    return Wei.fromHexString(hex);
  }

  private static Address address(final String hex) {
    return Address.fromHexString(hex);
  }
}
