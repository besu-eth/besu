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
package org.hyperledger.besu.ethereum.eth.transactions.inclusionlist;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lenient inclusion list validator for testing environments. Logs warnings for constraint
 * violations but always returns VALID, allowing non-compliant payloads to proceed.
 */
public class LenientInclusionListValidator implements InclusionListValidator {

  private static final Logger LOG = LoggerFactory.getLogger(LenientInclusionListValidator.class);

  private final AtomicLong violationCount = new AtomicLong(0);

  @Override
  public InclusionListValidationResult validate(
      final List<Bytes> payloadTransactions,
      final List<Bytes> inclusionListTransactions,
      final InclusionListValidationContext context) {

    if (inclusionListTransactions == null || inclusionListTransactions.isEmpty()) {
      return InclusionListValidationResult.valid();
    }

    // Check total byte size
    final int totalBytes = inclusionListTransactions.stream().mapToInt(Bytes::size).sum();
    if (totalBytes > InclusionListConfiguration.MAX_BYTES_PER_INCLUSION_LIST) {
      violationCount.incrementAndGet();
      LOG.warn(
          "Inclusion list exceeds MAX_BYTES_PER_INCLUSION_LIST: {} > {} (lenient mode - accepting)",
          totalBytes,
          InclusionListConfiguration.MAX_BYTES_PER_INCLUSION_LIST);
      return InclusionListValidationResult.valid();
    }

    // Check presence of all IL transactions in payload (anywhere-in-block per EIP-7805)
    final Set<Bytes> payloadTxSet = new HashSet<>(payloadTransactions);
    for (int i = 0; i < inclusionListTransactions.size(); i++) {
      final Bytes ilTx = inclusionListTransactions.get(i);
      if (!payloadTxSet.contains(ilTx)) {
        violationCount.incrementAndGet();
        LOG.warn(
            "Inclusion list not satisfied: missing transaction at index {} ({}) (lenient mode - accepting)",
            i,
            ilTx.toHexString());
        return InclusionListValidationResult.valid();
      }
    }

    return InclusionListValidationResult.valid();
  }

  /** Returns the total number of violations encountered. */
  public long getViolationCount() {
    return violationCount.get();
  }
}
