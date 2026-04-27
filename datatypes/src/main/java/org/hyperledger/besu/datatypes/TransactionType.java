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
package org.hyperledger.besu.datatypes;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/** The enum Transaction type. */
public enum TransactionType {
  /** The Frontier. */
  FRONTIER(0xf8, 0x00),
  /** Access list transaction type. */
  ACCESS_LIST(0x01),
  /** Eip1559 transaction type. */
  EIP1559(0x02),
  /** Blob transaction type. */
  BLOB(0x03),
  /** Eip7702 transaction type. */
  DELEGATE_CODE(0x04);

  private static final Set<TransactionType> ACCESS_LIST_SUPPORTED_TRANSACTION_TYPES =
      EnumSet.of(ACCESS_LIST, EIP1559, BLOB, DELEGATE_CODE);

  private static final Set<TransactionType> LEGACY_FEE_MARKET_TRANSACTION_TYPES =
      EnumSet.of(FRONTIER, ACCESS_LIST);

  // The theoretical boundaries of the first byte of the RLP of a Frontier transaction.
  // In practice Frontier transactions almost always start with 0xf8 or 0xf9.
  private static final byte MIN_LEGACY_TX_OPAQUE_BYTE = (byte) 0xc0;
  private static final byte MAX_LEGACY_TX_OPAQUE_BYTE = (byte) 0xfe;

  // This array helps to translate the first opaque byte of an RLP encoded transaction
  // to its type.
  // Note that Frontier type occupies more slots, since there are different first byte values,
  // that represent such type.
  // Holes in the array represents invalid first byte values, for which there is not (yet)
  // a defined transaction type
  private static final TransactionType[] transactionTypeByOpaqueByte =
      new TransactionType[Byte.toUnsignedInt(MAX_LEGACY_TX_OPAQUE_BYTE) + 1];

  private static final TransactionType[] transactionTypeByEthSerializedType =
      new TransactionType[values().length];

  static {
    EnumSet.allOf(TransactionType.class).stream()
        .forEach(
            tt -> {
              tt.requireChainId = tt != FRONTIER;
              tt.supportAccessList = ACCESS_LIST_SUPPORTED_TRANSACTION_TYPES.contains(tt);
              tt.supportBaseFeeMarket = !LEGACY_FEE_MARKET_TRANSACTION_TYPES.contains(tt);
              tt.supportBlob = tt == BLOB;
              tt.supportDelegatedCode = tt == DELEGATE_CODE;
              if (tt == FRONTIER) {
                for (int i = Byte.toUnsignedInt(MIN_LEGACY_TX_OPAQUE_BYTE);
                    i < Byte.toUnsignedInt(MAX_LEGACY_TX_OPAQUE_BYTE);
                    i++) {
                  transactionTypeByOpaqueByte[i] = FRONTIER;
                }
              } else {
                transactionTypeByOpaqueByte[tt.getSerializedType()] = tt;
              }
              transactionTypeByEthSerializedType[tt.getEthSerializedType()] = tt;
            });
  }

  private final byte typeValue;
  private final byte serializedType;
  boolean requireChainId;
  boolean supportAccessList;
  boolean supportBaseFeeMarket;
  boolean supportBlob;
  boolean supportDelegatedCode;

  TransactionType(final int typeValue, final int serializedType) {
    this.typeValue = (byte) typeValue;
    this.serializedType = (byte) serializedType;
  }

  TransactionType(final int typeValue) {
    this(typeValue, typeValue);
  }

  /**
   * Gets serialized type.
   *
   * @return the serialized type
   */
  public byte getSerializedType() {
    return typeValue;
  }

  /**
   * Gets serialized type for returning in an eth transaction result, factoring in the special case
   * for FRONTIER transactions which have enum type 0xf8 but are represented as 0x00 in transaction
   * results.
   *
   * @return the serialized type
   */
  public byte getEthSerializedType() {
    return serializedType;
  }

  /**
   * Convert from opaque byte to type value.
   *
   * @param opaqueByte the opaque byte from serialized bytes
   * @return the transaction type
   */
  public static Optional<TransactionType> fromOpaque(final byte opaqueByte) {
    try {
      return Optional.ofNullable(transactionTypeByOpaqueByte[Byte.toUnsignedInt(opaqueByte)]);
    } catch (final ArrayIndexOutOfBoundsException e) {
      return Optional.empty();
    }
  }

  /**
   * Convert from ETH serialized byte to type value.
   *
   * @param ethSerializedType the opaque byte from serialized bytes
   * @return the transaction type
   */
  public static Optional<TransactionType> fromEthSerializedType(final byte ethSerializedType) {
    try {
      return Optional.ofNullable(transactionTypeByEthSerializedType[ethSerializedType]);
    } catch (final ArrayIndexOutOfBoundsException e) {
      return Optional.empty();
    }
  }

  /**
   * Does transaction type support access list.
   *
   * @return the boolean
   */
  public boolean supportsAccessList() {
    return supportAccessList;
  }

  /**
   * Does transaction type support EIP-1559 fee market.
   *
   * @return the boolean
   */
  public boolean supports1559FeeMarket() {
    return supportBaseFeeMarket;
  }

  /**
   * Does transaction type require chain id.
   *
   * @return the boolean
   */
  public boolean requiresChainId() {
    return requireChainId;
  }

  /**
   * Does transaction type support data blobs.
   *
   * @return the boolean
   */
  public boolean supportsBlob() {
    return supportBlob;
  }

  /**
   * Does transaction type support delegate code.
   *
   * @return the boolean
   */
  public boolean supportsDelegateCode() {
    return supportDelegatedCode;
  }
}
