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

import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;

import org.apache.tuweni.bytes.Bytes32;

public class TrieIterator<V> implements PathNodeVisitor<V> {

  private static final byte[][] SINGLE_NIBBLES = new byte[16][];

  static {
    for (int i = 0; i < SINGLE_NIBBLES.length; i++) {
      SINGLE_NIBBLES[i] = new byte[] {(byte) i};
    }
  }

  private final Deque<byte[]> paths = new ArrayDeque<>();
  private final LeafHandler<V> leafHandler;
  private State state = State.SEARCHING;
  private final boolean unload;

  public TrieIterator(final LeafHandler<V> leafHandler, final boolean unload) {
    this.leafHandler = leafHandler;
    this.unload = unload;
  }

  @Override
  public Node<V> visit(final ExtensionNode<V> node, final byte[] searchPath, final int offset) {
    byte[] remainingPath = searchPath;
    int remainingOffset = offset;
    if (state == State.SEARCHING) {
      final byte[] extensionPath = node.path();
      final int commonPrefixEnd =
          offset + Math.min(searchPath.length - offset, extensionPath.length);
      remainingOffset = commonPrefixEnd;
      if (Arrays.compareUnsigned(
              extensionPath, 0, extensionPath.length, searchPath, offset, commonPrefixEnd)
          > 0) {
        remainingPath = new byte[searchPath.length - commonPrefixEnd];
        remainingOffset = 0;
      }
    }
    paths.push(node.path());
    node.getChild().accept(this, remainingPath, remainingOffset);
    if (unload) {
      node.getChild().unload();
    }
    paths.pop();
    return node;
  }

  @Override
  public Node<V> visit(final BranchNode<V> node, final byte[] searchPath, final int offset) {
    byte iterateFrom = 0;
    int remainingOffset = offset;
    if (state == State.SEARCHING) {
      iterateFrom = searchPath[offset];
      if (iterateFrom == CompactEncoding.LEAF_TERMINATOR) {
        return node;
      }
      remainingOffset = offset + 1;
    }
    paths.push(node.path());
    for (int i = iterateFrom; i < node.maxChild() && state.continueIterating(); i++) {
      paths.push(SINGLE_NIBBLES[i]);
      final Node<V> child = node.child((byte) i);
      if (i == iterateFrom) {
        child.accept(this, searchPath, remainingOffset);
      } else {
        child.accept(this, new byte[searchPath.length - remainingOffset], 0);
      }
      if (unload) {
        child.unload();
      }
      paths.pop();
    }
    paths.pop();
    return node;
  }

  @Override
  public Node<V> visit(final LeafNode<V> node, final byte[] path, final int offset) {
    paths.push(node.path());
    state = State.CONTINUE;
    state = leafHandler.onLeaf(keyHash(), node);
    paths.pop();
    return node;
  }

  @Override
  public Node<V> visit(final NullNode<V> node, final byte[] path, final int offset) {
    state = State.CONTINUE;
    return node;
  }

  private Bytes32 keyHash() {
    int length = 0;
    for (final byte[] path : paths) {
      length += path.length;
    }
    final byte[] fullPath = new byte[length];
    int pos = 0;
    final Iterator<byte[]> iterator = paths.descendingIterator();
    while (iterator.hasNext()) {
      final byte[] path = iterator.next();
      System.arraycopy(path, 0, fullPath, pos, path.length);
      pos += path.length;
    }
    return isZero(fullPath) ? Bytes32.ZERO : Bytes32.wrap(CompactEncoding.pathToBytes(fullPath), 0);
  }

  private static boolean isZero(final byte[] path) {
    for (final byte nibble : path) {
      if (nibble != 0) {
        return false;
      }
    }
    return true;
  }

  public interface LeafHandler<V> {

    State onLeaf(Bytes32 keyHash, Node<V> node);
  }

  public enum State {
    SEARCHING,
    CONTINUE,
    STOP;

    public boolean continueIterating() {
      return this != STOP;
    }
  }
}
