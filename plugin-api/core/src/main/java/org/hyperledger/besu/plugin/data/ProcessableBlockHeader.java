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
package org.hyperledger.besu.plugin.data;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Quantity;
import org.hyperledger.besu.plugin.Unstable;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

/**
 * The minimum set of data for a BlockHeader, as defined in the <a href=
 * "https://ethereum.github.io/yellowpaper/paper.pdf">Ethereum Yellow Paper</a>.
 */
public interface ProcessableBlockHeader {

  /**
   * The Keccak 256-bit hash of the parent block’s header, in its entirety.
   *
   * @return The Keccak 256-bit hash of the parent block’s header, in its entirety.
   */
  Hash getParentHash();

  /**
   * The 160-bit address to which all fees collected from the successful mining of this block be
   * transferred.
   *
   * <p>The name in the yellow paper is beneficiary.
   *
   * @return The 160-bit address to which all fees collected from the successful mining of this
   *     block be transferred.
   */
  Address getCoinbase();

  /**
   * A scalar value corresponding to the difficulty level of this block. This can be calculated from
   * the previous block’s difficulty level and the timestamp.
   *
   * @return A scalar value corresponding to the difficulty level of this block. This can be
   *     calculated from the previous block’s difficulty level and the timestamp.
   */
  Quantity getDifficulty();

  /**
   * A scalar value equal to the number of ancestor blocks. The genesis block has a number of zero.
   *
   * @return A scalar value equal to the number of ancestor blocks. The genesis block has a number
   *     of zero.
   */
  long getNumber();

  /**
   * A scalar value equal to the current limit of gas expenditure per block.
   *
   * @return A scalar value equal to the current limit of gas expenditure per block.
   */
  long getGasLimit();

  /**
   * A scalar value equal to the reasonable output of Unix’s time() at this block’s inception.
   *
   * @return A scalar value equal to the reasonable output of Unix’s time() at this block’s
   *     inception.
   */
  long getTimestamp();

  /**
   * Optional 32 bytes of prevRandao data.
   *
   * @return Optional prevRandao bytes from this header.
   */
  default Optional<Bytes32> getPrevRandao() {
    return Optional.empty();
  }

  /**
   * The base fee of this block.
   *
   * @return The base fee of this block.
   */
  @Unstable
  default Optional<? extends Quantity> getBaseFee() {
    return Optional.empty();
  }

  /**
   * The parent beacon block root of this header.
   *
   * @return The parent_beacon_block_root of this header.
   */
  @Unstable
  Optional<? extends Bytes32> getParentBeaconBlockRoot();

  /**
   * The slot number of this header (EIP-7843).
   *
   * @return The slot number of this header.
   */
  @Unstable
  default Optional<Long> getOptionalSlotNumber() {
    return Optional.empty();
  }
}
