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
package org.hyperledger.besu.ethereum.core.json;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.Bytes48;
import org.apache.tuweni.units.bigints.UInt64;

public final class BesuJsonModule extends SimpleModule {

  public BesuJsonModule() {
    super("besu-json");

    addSerializer(Bytes.class, new BytesJson.Serializer());
    addDeserializer(Bytes.class, new BytesJson.Deserializer());

    addSerializer(Bytes32.class, new Bytes32Json.Serializer());
    addDeserializer(Bytes32.class, new Bytes32Json.Deserializer());

    addSerializer(Bytes48.class, new Bytes48Json.Serializer());
    addDeserializer(Bytes48.class, new Bytes48Json.Deserializer());

    addSerializer(UInt64.class, new UInt64Json.Serializer());
    addDeserializer(UInt64.class, new UInt64Json.Deserializer());

    addSerializer(Wei.class, new WeiJson.Serializer());
    addDeserializer(Wei.class, new WeiJson.Deserializer());

    addSerializer(BlobGas.class, new BlobGasJson.Serializer());
    addDeserializer(BlobGas.class, new BlobGasJson.Deserializer());

    addSerializer(LogsBloomFilter.class, new LogsBloomFilterJson.Serializer());
    addDeserializer(LogsBloomFilter.class, new LogsBloomFilterJson.Deserializer());

    addSerializer(Withdrawal.class, new WithdrawalJson.Serializer());
    addDeserializer(Withdrawal.class, new WithdrawalJson.Deserializer());

    addSerializer(BlockAccessList.class, new BlockAccessListJson.Serializer());
    addDeserializer(BlockAccessList.class, new BlockAccessListJson.Deserializer());
  }
}
