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
package org.hyperledger.besu.ethereum.mainnet.blockhash;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.mainnet.systemcall.InvalidSystemCallAddressException;
import org.hyperledger.besu.ethereum.mainnet.systemcall.SystemCallProcessor;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Processes the beacon block storage if it is present in the block header. */
public class CancunPreExecutionProcessor extends FrontierPreExecutionProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(CancunPreExecutionProcessor.class);
  Address BEACON_ROOTS_ADDRESS =
      Address.fromHexString("0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02");

  @Override
  public Void process(
      final BlockProcessingContext context,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    ProcessableBlockHeader currentBlockHeader = context.getBlockHeader();
    currentBlockHeader
        .getParentBeaconBlockRoot()
        .ifPresent(beaconRoot -> process(context, beaconRoot, accessLocationTracker));
    return null;
  }

  private void process(
      final BlockProcessingContext context,
      final Bytes32 beaconRootsAddress,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    SystemCallProcessor processor =
        new SystemCallProcessor(context.getProtocolSpec().getTransactionProcessor());
    try {
      processor.process(BEACON_ROOTS_ADDRESS, context, beaconRootsAddress, accessLocationTracker);
    } catch (InvalidSystemCallAddressException e) {
      // According to EIP-4788, fail silently if no code exists
      LOG.warn("Invalid system call address: {}", BEACON_ROOTS_ADDRESS);
    }
  }

  @Override
  public Optional<Address> getBeaconRootsContract() {
    return Optional.of(BEACON_ROOTS_ADDRESS);
  }
}
