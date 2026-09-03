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
package org.hyperledger.besu.ethereum;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Map;
import java.util.Set;

/**
 * Immutable snapshot of EIP-8025 code reads collected during block processing: the sets of
 * addresses whose code was read, and the ancestor block headers that were accessed.
 *
 * @param codeReads addresses whose bytecode was read during EVM execution
 * @param authorizationCodeReads addresses whose bytecode was read during EIP-7702 authorization
 *     processing
 * @param accessedAncestors block numbers and their hashes resolved via BLOCKHASH during execution
 */
public record WitnessCodeReads(
    Set<Address> codeReads,
    Set<Address> authorizationCodeReads,
    Map<Long, Hash> accessedAncestors) {}
