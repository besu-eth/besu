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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.core.json.Bytes32Json;
import org.hyperledger.besu.ethereum.core.json.BytesJson;
import org.hyperledger.besu.ethereum.core.json.QuantityJson;
import org.hyperledger.besu.plugin.data.BlockOverrides;

import java.math.BigInteger;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class BlockOverridesParameter extends BlockOverrides {
  /**
   * Constructs a new BlockOverrides instance.
   *
   * @param timestamp the optional timestamp
   * @param blockNumber the optional block number
   * @param blockHash the optional block hash
   * @param prevRandao the optional previous Randao
   * @param gasLimit the optional gas limit
   * @param feeRecipient the optional fee recipient
   * @param baseFeePerGas the optional base fee per gas
   * @param blobBaseFee the optional blob base fee
   * @param stateRoot the optional state root
   * @param difficulty the optional difficulty
   * @param extraData the optional extra data
   * @param mixHash the optional mix hash
   */
  @JsonCreator
  public BlockOverridesParameter(
      @JsonProperty("time") final Optional<UnsignedLongParameter> timestamp,
      @JsonProperty("number") final Optional<UnsignedLongParameter> blockNumber,
      @JsonProperty("hash") final Optional<Hash> blockHash,
      @JsonProperty("prevRandao") final Optional<String> prevRandao,
      @JsonProperty("gasLimit") final Optional<UnsignedLongParameter> gasLimit,
      @JsonProperty("feeRecipient") final Optional<Address> feeRecipient,
      @JsonProperty("baseFeePerGas") final Optional<Wei> baseFeePerGas,
      @JsonProperty("blobBaseFee") final Optional<Wei> blobBaseFee,
      @JsonProperty("stateRoot") final Optional<Hash> stateRoot,
      @JsonProperty("difficulty") final Optional<BigInteger> difficulty,
      @JsonProperty("extraData") final Optional<Bytes> extraData,
      @JsonProperty("mixHash") final Optional<String> mixHash,
      @JsonProperty("parentBeaconBlockRoot") final Optional<String> parentBeaconBlockRoot) {
    super(
        timestamp,
        blockNumber,
        blockHash,
        gasLimit,
        feeRecipient,
        baseFeePerGas,
        blobBaseFee,
        stateRoot,
        difficulty,
        extraData,
        mixHash.isPresent() ? mixHash : prevRandao,
        parentBeaconBlockRoot);
  }

  @Override
  @JsonDeserialize(using = QuantityJson.OptionalLongDeserializer.class)
  public Optional<Long> getTimestamp() {
    return super.getTimestamp();
  }

  @Override
  @JsonDeserialize(using = QuantityJson.OptionalLongDeserializer.class)
  public Optional<Long> getBlockNumber() {
    return super.getBlockNumber();
  }

  @Override
  @JsonDeserialize(contentUsing = Bytes32Json.Deserializer.class)
  public Optional<Bytes32> getMixHashOrPrevRandao() {
    return super.getMixHashOrPrevRandao();
  }

  @Override
  @JsonDeserialize(contentUsing = BytesJson.Deserializer.class)
  public Optional<Bytes> getExtraData() {
    return super.getExtraData();
  }
}
