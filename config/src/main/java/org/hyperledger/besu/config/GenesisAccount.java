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
package org.hyperledger.besu.config;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

/**
 * Genesis account
 *
 * @param address of the account
 * @param nonce nonce of the account at genesis
 * @param balance balance of the account at genesis
 * @param code code of the account at genesis, can be null
 * @param storage storage of the account at genesis
 * @param privateKey of the account, only use for testing
 */
public record GenesisAccount(
    Address address,
    long nonce,
    Wei balance,
    @Nullable Bytes code,
    Map<UInt256, UInt256> storage,
    @Nullable Bytes32 privateKey) {}
