/*
 * Copyright 2020 ConsenSys AG.
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
package org.hyperledger.besu.tests.acceptance.bft;

import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeFactory;

import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

public class BftAcceptanceTestParameterization {

  public static Stream<Arguments> getFactories() {
    return Stream.of(
        Arguments.of(
            "ibft2",
            new BftAcceptanceTestParameterization(
                BesuNodeFactory::createIbft2Node,
                BesuNodeFactory::createIbft2NodeFixedPort,
                BesuNodeFactory::createIbft2NodeWithValidators)),
        Arguments.of(
            "qbft",
            new BftAcceptanceTestParameterization(
                BesuNodeFactory::createQbftNode,
                BesuNodeFactory::createQbftNodeFixedPort,
                BesuNodeFactory::createQbftNodeWithValidators)));
  }

  @FunctionalInterface
  public interface NodeCreator {

    BesuNode create(BesuNodeFactory factory, String name, DataStorageFormat storageFormat)
        throws Exception;
  }

  @FunctionalInterface
  public interface NodeWithValidatorsCreator {

    BesuNode create(BesuNodeFactory factory, String name, String[] validators) throws Exception;
  }

  private final NodeCreator creatorFn;
  private final NodeCreator fixedPortCreatorFn;
  private final NodeWithValidatorsCreator createorWithValidatorFn;

  public BftAcceptanceTestParameterization(
      final NodeCreator creatorFn,
      final NodeCreator fixedPortCreatorFn,
      final NodeWithValidatorsCreator createorWithValidatorFn) {
    this.creatorFn = creatorFn;
    this.fixedPortCreatorFn = fixedPortCreatorFn;
    this.createorWithValidatorFn = createorWithValidatorFn;
  }

  public BesuNode createNode(final BesuNodeFactory factory, final String name) throws Exception {
    return creatorFn.create(factory, name, DataStorageFormat.FOREST);
  }

  public BesuNode createBonsaiNode(final BesuNodeFactory factory, final String name)
      throws Exception {
    return creatorFn.create(factory, name, DataStorageFormat.BONSAI);
  }

  public BesuNode createBonsaiArchiveNode(final BesuNodeFactory factory, final String name)
      throws Exception {
    return creatorFn.create(factory, name, DataStorageFormat.X_BONSAI_ARCHIVE);
  }

  public BesuNode createForestNode(final BesuNodeFactory factory, final String name)
      throws Exception {
    return creatorFn.create(factory, name, DataStorageFormat.FOREST);
  }

  // Fixed-port variants below. These give each node a deterministic, restart-stable p2p/RPC port
  // and are intended for tests that stop and restart nodes
  public BesuNode createBonsaiNodeFixedPort(final BesuNodeFactory factory, final String name)
      throws Exception {
    return fixedPortCreatorFn.create(factory, name, DataStorageFormat.BONSAI);
  }

  public BesuNode createBonsaiArchiveNodeFixedPort(final BesuNodeFactory factory, final String name)
      throws Exception {
    return fixedPortCreatorFn.create(factory, name, DataStorageFormat.X_BONSAI_ARCHIVE);
  }

  public BesuNode createForestNodeFixedPort(final BesuNodeFactory factory, final String name)
      throws Exception {
    return fixedPortCreatorFn.create(factory, name, DataStorageFormat.FOREST);
  }

  public BesuNode createNodeWithValidators(
      final BesuNodeFactory factory, final String name, final String[] validators)
      throws Exception {
    return createorWithValidatorFn.create(factory, name, validators);
  }
}
