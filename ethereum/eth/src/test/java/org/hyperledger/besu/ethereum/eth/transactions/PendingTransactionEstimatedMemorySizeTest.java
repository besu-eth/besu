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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.ACCESS_LIST_ENTRY_SHALLOW_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.ACCESS_LIST_STORAGE_KEY_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.BIG_CHAIN_ID_MAX_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.BLOBS_WITH_COMMITMENTS_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.BLOB_PROOF_BUNDLE_SIZE_V0;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.BLOB_PROOF_BUNDLE_SIZE_V1;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.CHAIN_ID_SHALLOW_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.CODE_DELEGATION_ENTRY_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.EIP1559_AND_EIP4844_SHALLOW_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.FRONTIER_AND_ACCESS_LIST_SHALLOW_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.KZG_PROOF_CONTAINER_SHALLOW_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.KZG_PROOF_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.OPTIONAL_ACCESS_LIST_SHALLOW_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.OPTIONAL_CODE_DELEGATION_LIST_SHALLOW_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.OPTIONAL_TO_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.PAYLOAD_SHALLOW_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.PENDING_TRANSACTION_SHALLOW_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MemorySize.calculateListShallowSize;

import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ChainId;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.eth.transactions.layered.BaseTransactionPoolTest;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.LongAdder;
import javax.management.MBeanServer;

import com.google.common.collect.Sets;
import com.sun.management.HotSpotDiagnosticMXBean;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.info.GraphPathRecord;
import org.openjdk.jol.info.GraphVisitor;
import org.openjdk.jol.info.GraphWalker;

/**
 * This test class has a double utility, first it is used to verify that the current memory size of
 * a pending transaction object correspond to the calculated values, so if any of the tests in this
 * file fails, it probably means that one of the related classes has changed its format, and a new
 * calculation needs to be done.
 *
 * <p>The second utility is to help with the calculation of the memory size of a class, using the <a
 * href="https://github.com/openjdk/jol">JOL Tool</a>, to navigate the class layout and collect the
 * reported memory sizes.
 *
 * <p>For a correct calculation there are some things to consider, first exclude from the
 * calculation any reference to a <i>constant</i> object, for example if a field is always a
 * reference to {@code Optional.empty()} then just count the reference size, but not the size of the
 * Optional since it is always the same instance for every pending transaction.
 *
 * <p>To study the layout of a class it is usually useful to create a test method with one or more
 * instance of it, then programmatically save a heap dump using the {@link #dumpHeap(String,
 * boolean)} method, then analyze the heap dump with a tool to identify which are the dynamic and
 * constant fields, then use the JOL Tool to print the class layout and walk in the object tree,
 * then complete the writing of the test that will verify the current amount of memory used by that
 * class.
 */
public class PendingTransactionEstimatedMemorySizeTest extends BaseTransactionPoolTest {
  /**
   * Classes that represent constant instances, across all pending transaction types, and are
   * ignored during the calculation
   */
  private static final Set<Class<?>> SHARED_CLASSES =
      Set.of(SignatureAlgorithm.class, TransactionType.class, BlobType.class);

  /**
   * Field that points to constant values, across all pending transaction types, and are ignored
   * during the calculation
   */
  private static final Set<String> COMMON_CONSTANT_FIELD_PATHS =
      Set.of(".value.ctor", ".hashNoSignature", ".signature.encoded.delegate", ".chainId");

  /**
   * Field that points to constant values, for EIP-1559 and EIP-4844 pending transactions, and are
   * ignored during the calculation
   */
  private static final Set<String> EIP1559_EIP4844_CONSTANT_FIELD_PATHS =
      Sets.union(COMMON_CONSTANT_FIELD_PATHS, Set.of(".gasPrice"));

  /**
   * Field that points to constant values, for Frontier and Access List pending transactions, and
   * are ignored during the calculation
   */
  private static final Set<String> FRONTIER_ACCESS_LIST_CONSTANT_FIELD_PATHS =
      Sets.union(COMMON_CONSTANT_FIELD_PATHS, Set.of(".maxFeePerGas", ".maxPriorityFeePerGas"));

  /** Field which value is dynamic and can only be calculated at runtime */
  private static final Set<String> TX_VARIABLE_SIZE_PATHS =
      Set.of(
          ".to",
          ".payload",
          ".maybeAccessList",
          ".versionedHashes",
          ".blobsWithCommitments",
          ".maybeCodeDelegationList");

  @Test
  public void toSize() {
    TransactionTestFixture preparedTx =
        prepareTransaction(
            TransactionType.ACCESS_LIST,
            10,
            Wei.of(500),
            Wei.ZERO,
            10,
            0,
            BlobType.KZG_PROOF,
            null);
    Transaction txTo =
        preparedTx.to(Optional.of(Address.extract(Bytes32.random()))).createTransaction(KEYS1);
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    txTo.writeTo(rlpOut);

    txTo = Transaction.readFrom(new BytesValueRLPInput(rlpOut.encoded(), false)).detachedCopy();
    System.out.println(txTo.getSender());
    System.out.println(txTo.getHash());
    System.out.println(txTo.getSizeForAnnouncement());

    Optional<Address> to = txTo.getTo();
    final ClassLayout cl = ClassLayout.parseInstance(to);
    System.out.println(cl.toPrintable());
    final long maybeToSize = retainedSizeOfField(to, Set.of(), Set.of());

    System.out.println("Optional To size: " + maybeToSize);

    assertThat(maybeToSize).isEqualTo(OPTIONAL_TO_SIZE);
  }

  @Test
  public void payloadSize() {

    TransactionTestFixture preparedTx =
        prepareTransaction(
            TransactionType.ACCESS_LIST,
            10,
            Wei.of(500),
            Wei.ZERO,
            10,
            0,
            BlobType.KZG_PROOF,
            null);
    Transaction txPayload = preparedTx.createTransaction(KEYS1);
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    txPayload.writeTo(rlpOut);

    txPayload =
        Transaction.readFrom(new BytesValueRLPInput(rlpOut.encoded(), false)).detachedCopy();
    System.out.println(txPayload.getSender());
    System.out.println(txPayload.getHash());
    System.out.println(txPayload.getSizeForAnnouncement());

    final Bytes payload = txPayload.getPayload();
    final ClassLayout cl = ClassLayout.parseInstance(payload);
    System.out.println(cl.toPrintable());
    final long size = cl.instanceSize();
    System.out.println("Base payload size: " + size);

    assertThat(size).isEqualTo(PAYLOAD_SHALLOW_SIZE);
  }

  @Test
  public void chainIdIsSharedWithTheNetworkOne() {
    final BigInteger chainId = BigInteger.valueOf(1337);
    final Transaction tx =
        prepareTransaction(
                TransactionType.DELEGATE_CODE,
                0,
                Wei.of(500),
                Wei.ZERO,
                0,
                0,
                BlobType.KZG_PROOF,
                List.of(
                    // valid on this chain only, and on any chain
                    TransactionTestFixture.createSignedCodeDelegation(
                        chainId, Address.ZERO, 0, KEYS1),
                    TransactionTestFixture.createSignedCodeDelegation(
                        BigInteger.ZERO, Address.ZERO, 1, KEYS1)))
            .chainId(Optional.of(chainId))
            .createTransaction(KEYS1);
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    tx.writeTo(rlpOut);

    // done at startup by the node
    final ChainId networkChainId = ChainId.initNetworkChainId(ChainId.of(chainId));
    try {
      final Transaction pendingTx =
          Transaction.readFrom(new BytesValueRLPInput(rlpOut.encoded(), false)).detachedCopy();

      // the chain id of the pending transactions, and of their code delegations, is always the
      // instance of the network chain id, so it is not counted in their memory size
      assertThat(pendingTx.getChainIdentifier()).isSameAs(networkChainId);
      final var codeDelegations = pendingTx.getCodeDelegationList().orElseThrow();
      assertThat(ChainId.fromCodeDelegation(codeDelegations.get(0))).isSameAs(networkChainId);
      assertThat(ChainId.fromCodeDelegation(codeDelegations.get(1))).isSameAs(ChainId.of(0));
    } finally {
      ChainId.resetNetworkChainId();
    }
  }

  @Test
  public void chainIdOfCodeDelegationForAnotherChainIsCounted() {
    System.setProperty("jol.magicFieldOffset", "true");
    final BigInteger chainId = BigInteger.valueOf(1337);
    final BigInteger twoPow256MinusOne = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);

    ChainId.initNetworkChainId(ChainId.of(chainId));
    try {
      // valid on any chain, so it shares the chain id instance like one for this chain
      final PendingTransaction sharedChainId =
          new PendingTransaction.Remote(decodedCodeDelegationTx(chainId, BigInteger.ZERO));
      assertThat(
              new PendingTransaction.Remote(decodedCodeDelegationTx(chainId, chainId)).memorySize())
          .isEqualTo(sharedChainId.memorySize());

      // a chain id that fits in a long
      final Transaction longChainIdTx = decodedCodeDelegationTx(chainId, BigInteger.valueOf(42));
      final ChainId longChainId =
          ChainId.fromCodeDelegation(
              longChainIdTx.getCodeDelegationList().orElseThrow().getFirst());
      assertThat(GraphLayout.parseInstance(longChainId).totalSize())
          .isEqualTo(CHAIN_ID_SHALLOW_SIZE);
      assertThat(new PendingTransaction.Remote(longChainIdTx).memorySize())
          .isEqualTo(sharedChainId.memorySize() + CHAIN_ID_SHALLOW_SIZE);

      // chain ids that do not fit in a long, up to the largest allowed one
      for (final BigInteger bigValue :
          List.of(BigInteger.ONE.shiftLeft(64).add(BigInteger.ONE), twoPow256MinusOne)) {
        final Transaction bigChainIdTx = decodedCodeDelegationTx(chainId, bigValue);
        final ChainId bigChainId =
            ChainId.fromCodeDelegation(
                bigChainIdTx.getCodeDelegationList().orElseThrow().getFirst());
        assertThat(GraphLayout.parseInstance(bigChainId).totalSize())
            .isLessThanOrEqualTo(BIG_CHAIN_ID_MAX_SIZE);
        assertThat(new PendingTransaction.Remote(bigChainIdTx).memorySize())
            .isEqualTo(sharedChainId.memorySize() + BIG_CHAIN_ID_MAX_SIZE);
      }
      final ChainId largestChainId =
          ChainId.fromCodeDelegation(
              decodedCodeDelegationTx(chainId, twoPow256MinusOne)
                  .getCodeDelegationList()
                  .orElseThrow()
                  .getFirst());
      assertThat(GraphLayout.parseInstance(largestChainId).totalSize())
          .isEqualTo(BIG_CHAIN_ID_MAX_SIZE);
    } finally {
      ChainId.resetNetworkChainId();
    }
  }

  private Transaction decodedCodeDelegationTx(
      final BigInteger chainId, final BigInteger codeDelegationChainId) {
    final Transaction tx =
        prepareTransaction(
                TransactionType.DELEGATE_CODE,
                0,
                Wei.of(500),
                Wei.ZERO,
                0,
                0,
                BlobType.KZG_PROOF,
                List.of(
                    TransactionTestFixture.createSignedCodeDelegation(
                        codeDelegationChainId, Address.ZERO, 0, KEYS1)))
            .chainId(Optional.of(chainId))
            .createTransaction(KEYS1);
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    tx.writeTo(rlpOut);
    return Transaction.readFrom(new BytesValueRLPInput(rlpOut.encoded(), false)).detachedCopy();
  }

  @Test
  public void arrayMemoryEstimationByLength() {
    // start with 3 to avoid the special case of the List12
    for (int i = 3; i < 10; i++) {
      var array = new Object[i];
      Arrays.fill(array, new Object());
      var arrayList = List.copyOf(Arrays.asList(array));
      final long cSize = retainedSizeOfField(arrayList, Set.of(), Set.of(".elements["));

      assertThat(cSize).isEqualTo(calculateListShallowSize(arrayList.size()));
    }
  }

  @Test
  public void blobProofBundleSize_V0() {
    final int blobCount = 9;
    TransactionTestFixture preparedTx =
        prepareTransaction(
            TransactionType.BLOB,
            10,
            Wei.of(500),
            Wei.of(50),
            10,
            blobCount,
            BlobType.KZG_PROOF,
            null);
    Transaction txBlob = preparedTx.createTransaction(KEYS1);
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    TransactionEncoder.encodeRLP(txBlob, rlpOut, EncodingContext.POOLED_TRANSACTION);

    txBlob =
        TransactionDecoder.decodeRLP(
                new BytesValueRLPInput(rlpOut.encoded(), false), EncodingContext.POOLED_TRANSACTION)
            .detachedCopy();
    System.out.println(txBlob.getSender());
    System.out.println(txBlob.getHash());
    System.out.println(txBlob.getSizeForAnnouncement());

    final List<BlobProofBundle> blobProofBundles =
        txBlob.getBlobsWithCommitments().get().getBlobProofBundles();

    final long cSize = retainedSizeOfField(blobProofBundles, Set.of(), Set.of(".elements["));

    System.out.println("Container size: " + cSize);

    assertThat(cSize).isEqualTo(calculateListShallowSize(blobCount));

    final var blobProofBundle = blobProofBundles.getFirst();
    final long blobProofBundleSize = retainedSizeOfField(blobProofBundle, Set.of(), Set.of());

    System.out.println("BlobProofBundle size: " + blobProofBundleSize);

    assertThat(blobProofBundleSize).isEqualTo(BLOB_PROOF_BUNDLE_SIZE_V0);
  }

  @Test
  public void blobProofBundleSize_V1() {
    final int blobCount = 9;
    TransactionTestFixture preparedTx =
        prepareTransaction(
            TransactionType.BLOB,
            10,
            Wei.of(500),
            Wei.of(50),
            10,
            blobCount,
            BlobType.KZG_CELL_PROOFS,
            null);
    Transaction txBlob = preparedTx.createTransaction(KEYS1);
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    TransactionEncoder.encodeRLP(txBlob, rlpOut, EncodingContext.POOLED_TRANSACTION);

    txBlob =
        TransactionDecoder.decodeRLP(
                new BytesValueRLPInput(rlpOut.encoded(), false), EncodingContext.POOLED_TRANSACTION)
            .detachedCopy();
    System.out.println(txBlob.getSender());
    System.out.println(txBlob.getHash());
    System.out.println(txBlob.getSizeForAnnouncement());

    final List<BlobProofBundle> blobProofBundles =
        txBlob.getBlobsWithCommitments().get().getBlobProofBundles();

    final long blobProofBundlesSize =
        retainedSizeOfField(blobProofBundles, Set.of(), Set.of(".elements["));

    System.out.println("List of BlobProofBundle container size: " + blobProofBundlesSize);

    assertThat(blobProofBundlesSize).isEqualTo(calculateListShallowSize(blobCount));

    final var kzgProofList = blobProofBundles.getFirst().getKzgProof();
    // the list of proof is a sublist view on the single big shared list of all the proofs,
    // so we are just interested in the size of the sublist container here
    final long kzgProofContainerSize = retainedSizeOfField(kzgProofList, Set.of(), Set.of(".root"));
    System.out.println("KZGProof container size: " + kzgProofContainerSize);
    assertThat(kzgProofContainerSize).isEqualTo(KZG_PROOF_CONTAINER_SHALLOW_SIZE);

    final var kzgProof = kzgProofList.getFirst();
    final long kzgProofSize = retainedSizeOfField(kzgProof, Set.of(), Set.of());
    System.out.println("KZGProof size: " + kzgProofSize);
    assertThat(kzgProofSize).isEqualTo(KZG_PROOF_SIZE);

    final var blobProofBundle = blobProofBundles.getFirst();
    // get the size of the bundle w/o the dynamic part of the KZG proof list,
    // since it will be calculated at runtime
    final long blobProofBundleSize =
        retainedSizeOfField(blobProofBundle, Set.of(), Set.of(".kzgProof"));

    System.out.println("BlobProofBundle size: " + blobProofBundleSize);

    assertThat(blobProofBundleSize).isEqualTo(BLOB_PROOF_BUNDLE_SIZE_V1);
  }

  @Test
  public void blobsWithCommitmentsSize() {
    TransactionTestFixture preparedTx =
        prepareTransaction(
            TransactionType.BLOB, 10, Wei.of(500), Wei.of(50), 10, 1, BlobType.KZG_PROOF, null);
    Transaction txBlob = preparedTx.createTransaction(KEYS1);
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    TransactionEncoder.encodeRLP(txBlob, rlpOut, EncodingContext.POOLED_TRANSACTION);

    txBlob =
        TransactionDecoder.decodeRLP(
                new BytesValueRLPInput(rlpOut.encoded(), false), EncodingContext.POOLED_TRANSACTION)
            .detachedCopy();
    System.out.println(txBlob.getSender());
    System.out.println(txBlob.getHash());
    System.out.println(txBlob.getSizeForAnnouncement());

    final BlobsWithCommitments bwc = txBlob.getBlobsWithCommitments().get();
    final ClassLayout cl = ClassLayout.parseInstance(bwc);
    System.out.println(cl.toPrintable());
    System.out.println("BlobsWithCommitments size: " + cl.instanceSize());
    final ClassLayout rl = ClassLayout.parseInstance(bwc.getBlobProofBundles());
    System.out.println(rl.toPrintable());
    System.out.println("BlobProofBundle shallow list size:" + rl.instanceSize());

    assertThat(cl.instanceSize() + rl.instanceSize()).isEqualTo(BLOBS_WITH_COMMITMENTS_SIZE);
  }

  @Test
  public void pendingTransactionSize() {

    TransactionTestFixture preparedTx =
        prepareTransaction(
            TransactionType.ACCESS_LIST,
            10,
            Wei.of(500),
            Wei.ZERO,
            10,
            0,
            BlobType.KZG_PROOF,
            null);
    Transaction txPayload = preparedTx.createTransaction(KEYS1);
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    txPayload.writeTo(rlpOut);

    txPayload =
        Transaction.readFrom(new BytesValueRLPInput(rlpOut.encoded(), false)).detachedCopy();
    System.out.println(txPayload.getSender());
    System.out.println(txPayload.getHash());
    System.out.println(txPayload.getSizeForAnnouncement());

    final PendingTransaction pendingTx = new PendingTransaction.Remote(txPayload);

    final ClassLayout cl = ClassLayout.parseInstance(pendingTx);
    System.out.println(cl.toPrintable());
    final long size = cl.instanceSize();
    System.out.println("PendingTransaction size: " + size);

    assertThat(size).isEqualTo(PENDING_TRANSACTION_SHALLOW_SIZE);
  }

  @Test
  public void accessListSize() {
    System.setProperty("jol.magicFieldOffset", "true");

    final AccessListEntry ale1 =
        new AccessListEntry(Address.extract(Bytes32.random()), List.of(Bytes32.random()));

    final List<AccessListEntry> ales = List.of(ale1);

    TransactionTestFixture preparedTx =
        prepareTransaction(
            TransactionType.ACCESS_LIST, 0, Wei.of(500), Wei.ZERO, 0, 0, BlobType.KZG_PROOF, null);
    Transaction txAccessList = preparedTx.accessList(ales).createTransaction(KEYS1);
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    txAccessList.writeTo(rlpOut);

    txAccessList =
        Transaction.readFrom(new BytesValueRLPInput(rlpOut.encoded(), false)).detachedCopy();
    System.out.println(txAccessList.getSender());
    System.out.println(txAccessList.getHash());
    System.out.println(txAccessList.getSizeForAnnouncement());

    final var optAL = txAccessList.getAccessList();

    final ClassLayout optionalClassLayout = ClassLayout.parseInstance(optAL);
    System.out.println(optionalClassLayout.toPrintable());
    System.out.println("Optional size: " + optionalClassLayout.instanceSize());

    final ClassLayout listClassLayout = ClassLayout.parseInstance(optAL.get());
    System.out.println(listClassLayout.toPrintable());
    System.out.println(
        "Optional + list size: "
            + (optionalClassLayout.instanceSize() + listClassLayout.instanceSize()));

    assertThat(optionalClassLayout.instanceSize() + listClassLayout.instanceSize())
        .isEqualTo(OPTIONAL_ACCESS_LIST_SHALLOW_SIZE);

    final AccessListEntry ale = optAL.get().getFirst();

    long aleSize = retainedSizeOfField(ale, Set.of(), Set.of(".storageKeys.elements["));

    System.out.println("AccessListEntry container size: " + aleSize);

    assertThat(aleSize).isEqualTo(ACCESS_LIST_ENTRY_SHALLOW_SIZE);

    final Bytes32 storageKey = ale.storageKeys().getFirst();
    final ClassLayout cl4 = ClassLayout.parseInstance(storageKey);
    System.out.println(cl4.toPrintable());
    System.out.println("Single storage key size: " + cl4.instanceSize());

    assertThat(cl4.instanceSize()).isEqualTo(ACCESS_LIST_STORAGE_KEY_SIZE);
  }

  @Test
  public void codeDelegationListSize() {
    System.setProperty("jol.magicFieldOffset", "true");

    TransactionTestFixture preparedTx =
        prepareTransaction(
            TransactionType.DELEGATE_CODE,
            0,
            Wei.of(500),
            Wei.ZERO,
            0,
            0,
            BlobType.KZG_PROOF,
            List.of(CODE_DELEGATION_SENDER_1));
    Transaction txDelegateCode = preparedTx.createTransaction(KEYS1);
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    txDelegateCode.writeTo(rlpOut);

    txDelegateCode =
        Transaction.readFrom(new BytesValueRLPInput(rlpOut.encoded(), false)).detachedCopy();
    System.out.println(txDelegateCode.getSender());
    System.out.println(txDelegateCode.getHash());
    System.out.println(txDelegateCode.getSizeForAnnouncement());

    final var optCD = txDelegateCode.getCodeDelegationList();

    final ClassLayout optionalClassLayout = ClassLayout.parseInstance(optCD);
    System.out.println(optionalClassLayout.toPrintable());
    System.out.println("Optional size: " + optionalClassLayout.instanceSize());

    final ClassLayout listClassLayout = ClassLayout.parseInstance(optCD.get());
    System.out.println(listClassLayout.toPrintable());
    System.out.println(
        "Optional + list size: "
            + (optionalClassLayout.instanceSize() + listClassLayout.instanceSize()));

    assertThat(optionalClassLayout.instanceSize() + listClassLayout.instanceSize())
        .isEqualTo(OPTIONAL_CODE_DELEGATION_LIST_SHALLOW_SIZE);

    final CodeDelegation codeDelegation = optCD.get().getFirst();

    // the chain id is shared, see chainIdIsSharedWithTheNetworkOne, unless the code delegation is
    // for another chain, see chainIdOfCodeDelegationForAnotherChainIsCounted
    long cdSize = retainedSizeOfField(codeDelegation, Set.of(".chainId"), Set.of());
    System.out.println("CodeDelegation container size: " + cdSize);

    assertThat(cdSize).isEqualTo(CODE_DELEGATION_ENTRY_SIZE);
  }

  @Test
  public void baseEIP1559AndEIP4844TransactionMemorySize() {
    Transaction txEip1559 = createEIP1559Transaction(1, KEYS1, 10);
    assertThat(baseTransactionMemorySize(txEip1559, EIP1559_EIP4844_CONSTANT_FIELD_PATHS))
        .isEqualTo(EIP1559_AND_EIP4844_SHALLOW_SIZE);
  }

  @Test
  public void baseFrontierAndAccessListTransactionMemorySize() {
    final Transaction txFrontier =
        createTransaction(TransactionType.FRONTIER, 1, Wei.of(500), 0, List.of(), KEYS1);
    assertThat(baseTransactionMemorySize(txFrontier, FRONTIER_ACCESS_LIST_CONSTANT_FIELD_PATHS))
        .isEqualTo(FRONTIER_AND_ACCESS_LIST_SHALLOW_SIZE);
  }

  private long baseTransactionMemorySize(final Transaction tx, final Set<String> constantFields) {
    System.setProperty("jol.magicFieldOffset", "true");
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    tx.writeTo(rlpOut);

    final var baseTx =
        Transaction.readFrom(new BytesValueRLPInput(rlpOut.encoded(), false)).detachedCopy();
    System.out.println(baseTx.getSender());
    System.out.println(baseTx.getHash());
    System.out.println(baseTx.getSizeForAnnouncement());

    final ClassLayout cl = ClassLayout.parseInstance(baseTx);
    System.out.println(cl.toPrintable());
    final long baseTxSize = retainedSizeOfField(baseTx, constantFields, TX_VARIABLE_SIZE_PATHS);

    System.out.println("Base tx size: " + baseTxSize);
    return baseTxSize;
  }

  private GraphWalker getGraphWalker(
      final Set<String> constantFieldPaths,
      final Set<String> variableSizePaths,
      final SortedSet<FieldSize> fieldSizes) {
    final Set<String> skipPrefixes = new HashSet<>();
    final Set<String> alreadyPrinted = new HashSet<>();
    final GraphVisitor gv =
        gpr -> {
          if (!startWithAnyOf(skipPrefixes, gpr)) {
            if (SHARED_CLASSES.stream().anyMatch(scz -> scz.isAssignableFrom(gpr.klass()))) {
              System.out.printf(
                  "Not counting field: %s, since of the shared class: %s%n",
                  gpr.path(), gpr.klass());
              skipPrefixes.add(gpr.path());
            } else if (startWithAnyOf(constantFieldPaths, gpr)) {
              var constantFieldPrefix = getMatchingPrefix(constantFieldPaths, gpr);
              if (!alreadyPrinted.contains(constantFieldPrefix)) {
                alreadyPrinted.add(constantFieldPrefix);
                System.out.println(
                    "Not counting constant size fields with prefix: " + constantFieldPrefix);
              }
            } else if (startWithAnyOf(variableSizePaths, gpr)) {
              var variableSizePrefix = getMatchingPrefix(variableSizePaths, gpr);
              if (!alreadyPrinted.contains(variableSizePrefix)) {
                alreadyPrinted.add(variableSizePrefix);
                System.out.println(
                    "Not counting variable size field with prefix: " + variableSizePrefix);
              }
            } else {
              System.out.printf("Counting field %s, size %s%n", gpr.path(), gpr.size());
              fieldSizes.add(new FieldSize(gpr.path(), gpr.klass(), gpr.size()));
            }
          }
        };

    GraphWalker gw = new GraphWalker(gv);
    return gw;
  }

  private boolean startWithAnyOf(final Set<String> prefixes, final GraphPathRecord path) {
    return prefixes.stream().anyMatch(prefix -> path.path().startsWith(prefix));
  }

  private String getMatchingPrefix(final Set<String> prefixes, final GraphPathRecord path) {
    return prefixes.stream().filter(prefix -> path.path().startsWith(prefix)).findFirst().get();
  }

  private long retainedSizeOfField(
      final Object container, final Set<String> constantFields, final Set<String> variableFields) {
    final ClassLayout cl = ClassLayout.parseInstance(container);
    System.out.println(cl.toPrintable());
    System.out.println("Base container size: " + cl.instanceSize());

    LongAdder size = new LongAdder();
    size.add(cl.instanceSize());

    final SortedSet<FieldSize> fieldSizes = new TreeSet<>();

    var gw = getGraphWalker(constantFields, variableFields, fieldSizes);

    gw.walk(container);

    fieldSizes.forEach(
        fieldSize -> {
          size.add(fieldSize.size());
          System.out.println(
              "("
                  + size
                  + ")["
                  + fieldSize.size()
                  + ", "
                  + fieldSize.path()
                  + ", "
                  + fieldSize
                  + "]");
        });
    System.out.println("Container size: " + size);
    return size.sum();
  }

  /**
   * Utility method useful for producing a heap dump when calculating the memory size of a new
   * object. Note that the file is not overwritten, so you need to remove it to create a new heap
   * dump.
   *
   * @param filePath where to save the heap dump
   * @param live true to only include live objects
   */
  @SuppressWarnings("unused")
  private static void dumpHeap(final String filePath, final boolean live) {
    try {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      HotSpotDiagnosticMXBean mxBean =
          ManagementFactory.newPlatformMXBeanProxy(
              server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
      mxBean.dumpHeap(filePath, live);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  record FieldSize(String path, Class<?> clazz, long size) implements Comparable<FieldSize> {

    @Override
    public int compareTo(final FieldSize o) {
      return path.compareTo(o.path);
    }
  }
}
