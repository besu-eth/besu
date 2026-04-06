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
package org.hyperledger.besu.ethereum.referencetests;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/** A Transaction test case specification. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeneralStateTestCaseSpec {

  private final Map<String, List<GeneralStateTestCaseEipSpec>> finalStateSpecs;
  private static final BlockHeaderFunctions MAINNET_FUNCTIONS = new MainnetBlockHeaderFunctions();

  @JsonCreator
  public GeneralStateTestCaseSpec(
      @JsonProperty("env") final ReferenceTestEnv blockHeader,
      @JsonProperty("pre") final ReferenceTestWorldState initialWorldState,
      @JsonProperty("post") final Map<String, List<PostSection>> postSection,
      @JsonProperty("transaction") final StateTestVersionedTransaction versionedTransaction) {
    this.finalStateSpecs =
        generate(blockHeader, initialWorldState, postSection, versionedTransaction);
  }

  private Map<String, List<GeneralStateTestCaseEipSpec>> generate(
      final BlockHeader rawBlockHeader,
      final ReferenceTestWorldState initialWorldState,
      final Map<String, List<PostSection>> postSections,
      final StateTestVersionedTransaction versionedTransaction) {
    if (initialWorldState == null) {
      return Map.of();
    }
    initialWorldState.persist(null);
    final Map<String, List<GeneralStateTestCaseEipSpec>> res =
        LinkedHashMap.newLinkedHashMap(postSections.size());
    for (final Map.Entry<String, List<PostSection>> entry : postSections.entrySet()) {
      final String eip = entry.getKey();
      final List<PostSection> post = entry.getValue();
      final List<GeneralStateTestCaseEipSpec> specs = new ArrayList<>(post.size());
      for (final PostSection p : post) {
        final BlockHeader blockHeader =
            BlockHeaderBuilder.fromHeader(rawBlockHeader)
                .stateRoot(p.rootHash)
                .blockHeaderFunctions(MAINNET_FUNCTIONS)
                .buildBlockHeader();
        final List<Supplier<Transaction>> txSupplierList =
            List.of(() -> versionedTransaction.get(p.indexes));
        specs.add(
            new GeneralStateTestCaseEipSpec(
                eip,
                txSupplierList,
                initialWorldState,
                p.rootHash,
                p.logsHash,
                blockHeader,
                p.indexes.data,
                p.indexes.gas,
                p.indexes.value,
                p.expectException));
      }
      res.put(eip, specs);
    }
    return res;
  }

  public Map<String, List<GeneralStateTestCaseEipSpec>> finalStateSpecs() {
    return finalStateSpecs;
  }

  /**
   * Indexes in the "transaction" part of the general state spec json, which allow tests to vary the
   * input transaction of the tests based on the hard-fork.
   */
  public static class Indexes {

    public final int gas;
    public final int data;
    public final int value;

    @JsonCreator
    public Indexes(
        @JsonProperty("gas") final int gas,
        @JsonProperty("data") final int data,
        @JsonProperty("value") final int value) {
      this.gas = gas;
      this.data = data;
      this.value = value;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      final Indexes other = (Indexes) obj;
      return data == other.data && gas == other.gas && value == other.value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(data, gas, value);
    }
  }

  /** Represents the "post" part of a general state test json _for a specific hard-fork_. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class PostSection {

    private final Hash rootHash;
    @Nullable private final Hash logsHash;
    private final Indexes indexes;
    private final String expectException;

    @JsonCreator
    public PostSection(
        @JsonProperty("expectException") final String expectException,
        @JsonProperty("hash") final String hash,
        @JsonProperty("indexes") final Indexes indexes,
        @JsonProperty("logs") final String logs,
        @JsonProperty("txbytes") final String txbytes) {
      this.rootHash = Hash.fromHexString(hash);
      this.logsHash = Optional.ofNullable(logs).map(Hash::fromHexString).orElse(null);
      this.indexes = indexes;
      this.expectException = expectException;
    }
  }
}
