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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

/**
 * Declarative rule set for an {@code engine_newPayloadVN} method version.
 *
 * <p>Each version of {@code engine_newPayload} supplies its own {@code NewPayloadRules} instance
 * describing which fields must be present, which are forbidden, which fork window the version
 * covers, and which optional logic to run (blob validation, block access list extraction).
 *
 * <p>The builder enforces that every field is set so adding a new version cannot silently inherit a
 * default rule.
 */
public record NewPayloadRules(
    ForkWindow forkWindow,
    FieldPresence blobGasUsed,
    FieldPresence excessBlobGas,
    FieldPresence versionedHashes,
    FieldPresence parentBeaconBlockRoot,
    FieldPresence executionRequests,
    FieldPresence slotNumber,
    BlobValidationMode blobValidation,
    BlockAccessListExtractor blockAccessListExtractor) {

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private ForkWindow forkWindow;
    private FieldPresence blobGasUsed;
    private FieldPresence excessBlobGas;
    private FieldPresence versionedHashes;
    private FieldPresence parentBeaconBlockRoot;
    private FieldPresence executionRequests;
    private FieldPresence slotNumber;
    private BlobValidationMode blobValidation;
    private BlockAccessListExtractor blockAccessListExtractor;

    private Builder() {}

    public Builder forkWindow(final ForkWindow forkWindow) {
      this.forkWindow = forkWindow;
      return this;
    }

    public Builder blobGasUsed(final FieldPresence presence) {
      this.blobGasUsed = presence;
      return this;
    }

    public Builder excessBlobGas(final FieldPresence presence) {
      this.excessBlobGas = presence;
      return this;
    }

    public Builder versionedHashes(final FieldPresence presence) {
      this.versionedHashes = presence;
      return this;
    }

    public Builder parentBeaconBlockRoot(final FieldPresence presence) {
      this.parentBeaconBlockRoot = presence;
      return this;
    }

    public Builder executionRequests(final FieldPresence presence) {
      this.executionRequests = presence;
      return this;
    }

    public Builder slotNumber(final FieldPresence presence) {
      this.slotNumber = presence;
      return this;
    }

    public Builder blobValidation(final BlobValidationMode mode) {
      this.blobValidation = mode;
      return this;
    }

    public Builder blockAccessListExtractor(final BlockAccessListExtractor extractor) {
      this.blockAccessListExtractor = extractor;
      return this;
    }

    public NewPayloadRules build() {
      requireSet(forkWindow, "forkWindow");
      requireSet(blobGasUsed, "blobGasUsed");
      requireSet(excessBlobGas, "excessBlobGas");
      requireSet(versionedHashes, "versionedHashes");
      requireSet(parentBeaconBlockRoot, "parentBeaconBlockRoot");
      requireSet(executionRequests, "executionRequests");
      requireSet(slotNumber, "slotNumber");
      requireSet(blobValidation, "blobValidation");
      requireSet(blockAccessListExtractor, "blockAccessListExtractor");
      return new NewPayloadRules(
          forkWindow,
          blobGasUsed,
          excessBlobGas,
          versionedHashes,
          parentBeaconBlockRoot,
          executionRequests,
          slotNumber,
          blobValidation,
          blockAccessListExtractor);
    }

    private static void requireSet(final Object value, final String fieldName) {
      if (value == null) {
        throw new IllegalStateException(
            "NewPayloadRules." + fieldName + " must be set before build()");
      }
    }
  }
}
