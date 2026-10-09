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
package org.hyperledger.besu.ethereum.trie;

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;

import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class SnapCommitVisitorTest {

  @Test
  public void shouldDetectValidBranch() {
    final StoredNodeFactory<Bytes> storedNodeFactory = mock(StoredNodeFactory.class);
    final ArrayList<Node<Bytes>> children = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      children.add(
          new StoredNode<>(
              storedNodeFactory,
              Bytes.concatenate(Bytes.of(0x00), Bytes.of(i)),
              Bytes32.wrap(Hash.ZERO.getBytes())));
    }
    final BranchNode<Bytes> validBranchNode =
        new BranchNode<>(
            new byte[] {0x00},
            children,
            Optional.of(Bytes.of(0x00)),
            storedNodeFactory,
            Function.identity());
    validBranchNode.markDirty();
    final SnapCommitVisitor<Bytes> snapCommitVisitor =
        new SnapCommitVisitor<>(
            (location, hash, value) -> {}, RangeManager.MIN_RANGE, RangeManager.MAX_RANGE);
    Assertions.assertThat(validBranchNode.isHealNeeded()).isFalse();
    snapCommitVisitor.visit(validBranchNode.location(), validBranchNode);
    Assertions.assertThat(validBranchNode.isHealNeeded()).isFalse();
  }

  @Test
  public void shouldDetectBranchWithChildrenNotInTheRange() {
    final StoredNodeFactory<Bytes> storedNodeFactory = mock(StoredNodeFactory.class);
    final ArrayList<Node<Bytes>> children = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      children.add(
          new StoredNode<>(
              storedNodeFactory,
              Bytes.concatenate(Bytes.of(0x01), Bytes.of(i)),
              Bytes32.wrap(Hash.ZERO.getBytes())));
    }

    final BranchNode<Bytes> invalidBranchNode =
        new BranchNode<>(
            new byte[] {0x01},
            children,
            Optional.of(Bytes.of(0x00)),
            storedNodeFactory,
            Function.identity());
    invalidBranchNode.markDirty();
    final SnapCommitVisitor<Bytes> snapCommitVisitor =
        new SnapCommitVisitor<>(
            (location, hash, value) -> {},
            RangeManager.MIN_RANGE,
            Bytes32.wrap(
                Hash.fromHexString(
                        "0x1effffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                    .getBytes()));
    Assertions.assertThat(invalidBranchNode.isHealNeeded()).isFalse();
    snapCommitVisitor.visit(invalidBranchNode.location(), invalidBranchNode);
    Assertions.assertThat(invalidBranchNode.isHealNeeded()).isTrue();
  }

  @Test
  public void shouldDetectValidExtension() {
    final StoredNodeFactory<Bytes> storedNodeFactory = mock(StoredNodeFactory.class);
    final ExtensionNode<Bytes> validExtensionNode =
        new ExtensionNode<>(
            new byte[] {0x00},
            new byte[] {0x01},
            new StoredNode<>(
                storedNodeFactory,
                Bytes.of((byte) 0x00, (byte) 0x01),
                Bytes32.wrap(Hash.ZERO.getBytes())),
            storedNodeFactory);
    validExtensionNode.markDirty();
    final SnapCommitVisitor<Bytes> snapCommitVisitor =
        new SnapCommitVisitor<>(
            (location, hash, value) -> {}, RangeManager.MIN_RANGE, RangeManager.MAX_RANGE);
    Assertions.assertThat(validExtensionNode.isHealNeeded()).isFalse();
    snapCommitVisitor.visit(validExtensionNode.location(), validExtensionNode);
    Assertions.assertThat(validExtensionNode.isHealNeeded()).isFalse();
  }

  @Test
  public void shouldDetectExtensionWithChildNotInRange() {
    final StoredNodeFactory<Bytes> storedNodeFactory = mock(StoredNodeFactory.class);
    final ExtensionNode<Bytes> inValidExtensionNode =
        new ExtensionNode<>(
            new byte[] {0x00},
            new byte[] {0x03},
            new StoredNode<>(
                storedNodeFactory,
                Bytes.of((byte) 0x00, (byte) 0x03),
                Bytes32.wrap(Hash.ZERO.getBytes())),
            storedNodeFactory);
    inValidExtensionNode.markDirty();
    final SnapCommitVisitor<Bytes> snapCommitVisitor =
        new SnapCommitVisitor<>(
            (location, hash, value) -> {},
            RangeManager.MIN_RANGE,
            Bytes32.wrap(
                Hash.fromHexString(
                        "0x02ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                    .getBytes()));
    Assertions.assertThat(inValidExtensionNode.isHealNeeded()).isFalse();
    snapCommitVisitor.visit(inValidExtensionNode.location(), inValidExtensionNode);
    Assertions.assertThat(inValidExtensionNode.isHealNeeded()).isTrue();
  }
}
