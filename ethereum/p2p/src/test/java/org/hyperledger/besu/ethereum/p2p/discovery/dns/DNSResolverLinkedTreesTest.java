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
package org.hyperledger.besu.ethereum.p2p.discovery.dns;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.Hash;

import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import com.google.common.io.Resources;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.crypto.SECP256K1;
import org.apache.tuweni.io.Base32;
import org.apache.tuweni.io.Base64URLSafe;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Linked enrtree trees are published by different parties under different keys. These tests build
 * small signed trees, the way go-ethereum's dnsdisc publishes them, and serve them from a mock DNS
 * server.
 */
@ExtendWith(VertxExtension.class)
class DNSResolverLinkedTreesTest {

  private final MockDnsServerVerticle dns = new MockDnsServerVerticle();
  private static List<String> enrs;
  private int nextEnr;

  @BeforeAll
  static void loadEnrs() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    final JsonObject fixture =
        new JsonObject(
            Resources.toString(
                Resources.getResource("discovery/dns/dns-records.json"), StandardCharsets.UTF_8));
    enrs = new ArrayList<>();
    for (final String name : fixture.fieldNames()) {
      final String content = fixture.getString(name);
      if (content.startsWith("enr:")) {
        enrs.add(content);
      }
    }
  }

  @BeforeEach
  void startDns(final Vertx vertx, final VertxTestContext context) {
    vertx.deployVerticle(dns).onComplete(context.succeedingThenComplete());
  }

  @Test
  void treeWithALowerSeqThanItsLinkedTreeIsStillReadOnLaterRefreshes(final Vertx vertx) {
    // the case that needs no attacker: two honest trees whose sequence numbers simply differ
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree child = new SignedTree("child.example");
    final List<String> childEnrs = takeEnrs(1);
    child.publish(100, childEnrs);
    final List<String> parentEnrs = takeEnrs(2);
    parent.publish(5, parentEnrs, child);
    final DNSResolver resolver = resolver(vertx, parent);

    assertThat(resolver.collectAll())
        .containsExactlyInAnyOrderElementsOf(records(parentEnrs, childEnrs));

    final List<String> newParentEnrs = takeEnrs(1);
    parent.publish(6, newParentEnrs, child);

    // only what changed: the parent's new ENRs, not the unchanged child's
    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(newParentEnrs));
    assertThat(resolver.sequence()).isEqualTo(6);
  }

  @Test
  void linkedTreeThatChangesIsReadEvenIfTheTreeLinkingToItDidNot(final Vertx vertx) {
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree child = new SignedTree("child.example");
    child.publish(100, takeEnrs(1));
    parent.publish(5, takeEnrs(1), child);
    final DNSResolver resolver = resolver(vertx, parent);
    resolver.collectAll();

    final List<String> newChildEnrs = takeEnrs(2);
    child.publish(101, newChildEnrs);

    assertThat(resolver.collectAll()).containsExactlyInAnyOrderElementsOf(records(newChildEnrs));
  }

  @Test
  void linkedTreeAtTheMaximumSeqDoesNotFreezeTheTreeLinkingToIt(final Vertx vertx) {
    // a linked tree's key holder can sign any sequence number for their own tree
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree hostile = new SignedTree("hostile.example");
    hostile.publish(Long.MAX_VALUE, takeEnrs(1));
    parent.publish(5, takeEnrs(1), hostile);
    final DNSResolver resolver = resolver(vertx, parent);
    resolver.collectAll();

    final List<String> newParentEnrs = takeEnrs(1);
    parent.publish(6, newParentEnrs, hostile);

    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(newParentEnrs));
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void cycleOfLinkedTreesIsReadOnce(final Vertx vertx) {
    final SignedTree first = new SignedTree("first.example");
    final SignedTree second = new SignedTree("second.example");
    final List<String> firstEnrs = takeEnrs(1);
    final List<String> secondEnrs = takeEnrs(1);
    // each links to the other, so following links alone would never end
    second.publish(1, secondEnrs, first);
    first.publish(1, firstEnrs, second);

    assertThat(resolver(vertx, first).collectAll())
        .containsExactlyInAnyOrderElementsOf(records(firstEnrs, secondEnrs));
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void subtreeReferencedByManyBranchesIsResolvedOnce(final Vertx vertx) {
    // each branch names the next one twice: 24 published branches would be 2^24 visits without
    // remembering what this refresh already resolved
    final SignedTree tree = new SignedTree("shared.example");
    final List<String> enr = takeEnrs(1);
    String label = tree.publishLeaf(enr.getFirst());
    for (int i = 0; i < 24; i++) {
      label = tree.publishLeaf("enrtree-branch:" + label + "," + label);
    }
    tree.publishRoot(1, label, tree.publishBranch(List.of()));

    assertThat(resolver(vertx, tree).collectAll()).containsExactlyElementsOf(records(enr));
  }

  @Test
  void linksAreFollowedAtMostMaxLinkDepthDeep(final Vertx vertx) {
    final List<SignedTree> chain = new ArrayList<>();
    final List<List<String>> chainEnrs = new ArrayList<>();
    for (int depth = 0; depth <= DNSResolver.MAX_LINK_DEPTH + 1; depth++) {
      chain.add(new SignedTree("depth" + depth + ".example"));
      chainEnrs.add(takeEnrs(1));
    }
    for (int depth = chain.size() - 1; depth >= 0; depth--) {
      if (depth == chain.size() - 1) {
        chain.get(depth).publish(1, chainEnrs.get(depth));
      } else {
        chain.get(depth).publish(1, chainEnrs.get(depth), chain.get(depth + 1));
      }
    }

    // depths 0 to MAX_LINK_DEPTH are read, the tree one link further is not
    final List<String> expected = new ArrayList<>();
    chainEnrs.subList(0, DNSResolver.MAX_LINK_DEPTH + 1).forEach(expected::addAll);
    assertThat(resolver(vertx, chain.getFirst()).collectAll())
        .containsExactlyInAnyOrderElementsOf(records(expected));
  }

  @Test
  void branchesAreFollowedAtMostMaxBranchDepthDeep(final Vertx vertx) {
    final SignedTree tree = new SignedTree("deep.example");
    final List<String> shallow = takeEnrs(1);
    final List<String> tooDeep = takeEnrs(1);
    // the enr root is depth 0, so the leaf under MAX_BRANCH_DEPTH branches is at depth
    // MAX_BRANCH_DEPTH + 1 and is not read; the shallow leaf next to the top branch is
    String label = tree.publishLeaf(tooDeep.getFirst());
    for (int i = 0; i < DNSResolver.MAX_BRANCH_DEPTH; i++) {
      label = tree.publishBranch(List.of(label));
    }
    final String enrRoot = tree.publishBranch(List.of(label, tree.publishLeaf(shallow.getFirst())));
    tree.publishRoot(1, enrRoot, tree.publishBranch(List.of()));

    assertThat(resolver(vertx, tree).collectAll()).containsExactlyElementsOf(records(shallow));
  }

  @Test
  void noMoreThanTheRecordBudgetIsResolvedInOneRefresh(final Vertx vertx) {
    final SignedTree tree = new SignedTree("big.example");
    final List<String> treeEnrs = takeEnrs(10);
    tree.publish(1, treeEnrs);
    // the root, then the enr and link branches, which are both one branch from the root, and two
    // leaves
    final DNSResolver resolver =
        new DNSResolver(vertx, tree.link(), 0, Optional.of("127.0.0.1:" + dns.port()), 5);

    assertThat(resolver.collectAll()).hasSize(2);
  }

  @Test
  void noMoreRecordsAreQueuedThanTheBudgetCanRead(final Vertx vertx) {
    // each branch names 30 records that do not exist: queuing all of them would hold 600 entries
    // after reading 24 records. Branches are kept small, as real ones are: some hosts drop larger
    // UDP answers.
    final SignedTree tree = new SignedTree("wide.example");
    final List<String> enr = takeEnrs(1);
    final List<String> labels = new ArrayList<>();
    labels.add(tree.publishLeaf(enr.getFirst()));
    for (int branch = 0; branch < 20; branch++) {
      final List<String> missing = new ArrayList<>();
      for (int record = 0; record < 30; record++) {
        missing.add("missing" + branch + "x" + record);
      }
      labels.add(tree.publishBranch(missing));
    }
    tree.publishRoot(1, tree.publishBranch(labels), tree.publishBranch(List.of()));
    final DNSResolver resolver =
        new DNSResolver(vertx, tree.link(), 0, Optional.of("127.0.0.1:" + dns.port()), 50);

    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(enr));
    // more than one branch of missing records was queued, but no more than the budget
    assertThat(resolver.largestQueueOfLastRefresh()).isBetween(31, 50);
  }

  @Test
  void recordNamedManyTimesIsQueuedOnce(final Vertx vertx) {
    final SignedTree tree = new SignedTree("repeated.example");
    final List<String> enr = takeEnrs(1);
    final String leaf = tree.publishLeaf(enr.getFirst());
    tree.publishRoot(
        1, tree.publishBranch(Collections.nCopies(30, leaf)), tree.publishBranch(List.of()));
    final DNSResolver resolver = resolver(vertx, tree);

    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(enr));
    // the link root and the leaf, not the leaf 30 times
    assertThat(resolver.largestQueueOfLastRefresh()).isEqualTo(2);
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void refreshStartedWhileAnotherIsRunningReadsNothing(final Vertx vertx) throws Exception {
    final SignedTree tree = new SignedTree("busy.example");
    final List<String> treeEnrs = takeEnrs(2);
    tree.publish(1, treeEnrs);
    final DNSResolver resolver = resolver(vertx, tree);
    dns.holdResponses();
    final List<List<EthereumNodeRecord>> firstResult = new ArrayList<>();
    final Thread first = new Thread(() -> firstResult.add(resolver.collectAll()));
    first.start();
    while (dns.receivedQueries() == 0) {
      Thread.sleep(10);
    }

    // the first refresh is waiting on DNS: a second one must neither read nor publish anything
    assertThat(resolver.collectAll()).isEmpty();

    dns.releaseResponses();
    first.join();
    assertThat(firstResult.getFirst()).containsExactlyInAnyOrderElementsOf(records(treeEnrs));
    assertThat(resolver.sequence()).isEqualTo(1);
  }

  @Test
  void linkedTreeThatCouldNotBeReadKeepsItsSequenceNumber(final Vertx vertx) {
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree child = new SignedTree("child.example");
    child.publish(100, takeEnrs(1));
    parent.publish(5, takeEnrs(1), child);
    final DNSResolver resolver = resolver(vertx, parent);
    resolver.collectAll();

    // the child's root cannot be resolved for one refresh
    dns.removeTxtRecord("child.example");
    assertThat(resolver.collectAll()).isEmpty();

    // then an older root of the child is served, validly signed but not newer than the one read
    child.publish(99, takeEnrs(1));
    assertThat(resolver.collectAll()).isEmpty();
  }

  @Test
  void olderRootDoesNotBringBackALinkANewerRootRemoved(final Vertx vertx) {
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree removed = new SignedTree("removed.example");
    removed.publish(1, takeEnrs(1));
    final String oldParentRoot = parent.publish(5, takeEnrs(1), removed);
    final DNSResolver resolver = resolver(vertx, parent);
    resolver.collectAll();

    final List<String> newParentEnrs = takeEnrs(1);
    parent.publish(6, newParentEnrs);
    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(newParentEnrs));

    // the old root, which still links to the removed tree, is served again
    dns.addTxtRecord("parent.example", oldParentRoot);
    assertThat(resolver.collectAll()).isEmpty();
    assertThat(resolver.sequence()).isEqualTo(6);
  }

  @Test
  void recordTooDeepOnOnePathIsStillReadThroughAShorterOne(final Vertx vertx) {
    // the same leaf sits below MAX_BRANCH_DEPTH branches and directly under the enr root; the
    // deep path is listed first, so a depth-first walk would reach it there and reject it
    final SignedTree tree = new SignedTree("paths.example");
    final List<String> enr = takeEnrs(1);
    final String leaf = tree.publishLeaf(enr.getFirst());
    String label = leaf;
    for (int i = 0; i < DNSResolver.MAX_BRANCH_DEPTH; i++) {
      label = tree.publishBranch(List.of(label));
    }
    tree.publishRoot(1, tree.publishBranch(List.of(label, leaf)), tree.publishBranch(List.of()));

    assertThat(resolver(vertx, tree).collectAll()).containsExactlyElementsOf(records(enr));
  }

  @Test
  void linkedTreeNotReachedBecauseARootLookupFailedKeepsItsSequenceNumber(final Vertx vertx) {
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree child = new SignedTree("child.example");
    child.publish(100, takeEnrs(1));
    final String parentRoot = parent.publish(5, takeEnrs(1), child);
    final DNSResolver resolver = resolver(vertx, parent);
    resolver.collectAll();

    // the root of the tree linking to the child cannot be resolved for one refresh
    dns.removeTxtRecord("parent.example");
    assertThat(resolver.collectAll()).isEmpty();

    dns.addTxtRecord("parent.example", parentRoot);
    child.publish(99, takeEnrs(1));
    assertThat(resolver.collectAll()).isEmpty();
  }

  @Test
  void linkedTreeNotReachedBecauseALinkLookupFailedKeepsItsSequenceNumber(final Vertx vertx) {
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree child = new SignedTree("child.example");
    child.publish(100, takeEnrs(1));
    final String parentRoot = parent.publish(5, takeEnrs(1), child);
    final DNSResolver resolver = resolver(vertx, parent);
    resolver.collectAll();

    // the branch listing the parent's links cannot be resolved for one refresh
    final String linkBranch = linkRoot(parentRoot) + ".parent.example";
    final String linkBranchContent = dns.txtRecord(linkBranch);
    dns.removeTxtRecord(linkBranch);
    assertThat(resolver.collectAll()).isEmpty();

    dns.addTxtRecord(linkBranch, linkBranchContent);
    child.publish(99, takeEnrs(1));
    assertThat(resolver.collectAll()).isEmpty();
  }

  @Test
  void linkedTreeNotReachedBecauseTheRecordBudgetRanOutKeepsItsSequenceNumber(final Vertx vertx) {
    // with one ENR each, the parent takes 6 records (its root, two branches, the ENR and the two
    // links) and each child 4, so a budget of 14 covers all three trees
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree first = new SignedTree("first.example");
    final SignedTree second = new SignedTree("second.example");
    first.publish(100, takeEnrs(1));
    second.publish(100, takeEnrs(1));
    parent.publish(1, takeEnrs(1), first, second);
    final DNSResolver resolver =
        new DNSResolver(vertx, parent.link(), 0, Optional.of("127.0.0.1:" + dns.port()), 14);
    resolver.collectAll();

    // the parent now has 9 ENRs and takes all 14 records: both children are queued, the budget
    // runs out at the first one, and the second is not reached
    final List<String> biggerParentEnrs = takeEnrs(9);
    parent.publish(2, biggerParentEnrs, first, second);
    assertThat(resolver.collectAll())
        .containsExactlyInAnyOrderElementsOf(records(biggerParentEnrs));

    final List<String> newParentEnrs = takeEnrs(1);
    parent.publish(3, newParentEnrs, first, second);
    second.publish(99, takeEnrs(1));
    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(newParentEnrs));
  }

  @Test
  void linkedTreeANewerRootRemovedIsForgottenOnceEverythingWasRead(final Vertx vertx) {
    // what is remembered is bounded by what the latest complete refresh reached
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree child = new SignedTree("child.example");
    child.publish(100, takeEnrs(1));
    parent.publish(5, takeEnrs(1), child);
    final DNSResolver resolver = resolver(vertx, parent);
    resolver.collectAll();

    final List<String> newParentEnrs = takeEnrs(1);
    parent.publish(6, newParentEnrs);
    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(newParentEnrs));

    // linked again later, with a tree its owner started over
    final List<String> restartedChildEnrs = takeEnrs(1);
    child.publish(1, restartedChildEnrs);
    parent.publish(7, List.of(), child);
    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(restartedChildEnrs));
  }

  @Test
  void linkSpellingATreeAlreadyReadDifferentlyDoesNotReadItAgain(final Vertx vertx) {
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree removed = new SignedTree("removed.example");
    final SignedTree child = new SignedTree("child.example");
    // the same tree as the parent, with its domain in capitals: the DNS answer for that spelling
    // carries an older root of the parent, still linking to a tree the current root removed
    final SignedTree parentInCapitals = parent.spelledAs("PARENT.example");
    removed.publish(1, takeEnrs(1));
    parentInCapitals.publish(5, takeEnrs(1), removed);
    // and as an absolute name, which resolves to the parent's current root
    final SignedTree parentAsAbsoluteName = parent.spelledAs("parent.example.");
    final List<String> childEnrs = takeEnrs(1);
    child.publish(1, childEnrs, parentInCapitals, parentAsAbsoluteName);
    final List<String> parentEnrs = takeEnrs(1);
    parent.publish(6, parentEnrs, child);

    assertThat(resolver(vertx, parent).collectAll())
        .containsExactlyInAnyOrderElementsOf(records(parentEnrs, childEnrs));
  }

  @Test
  void treeLeftPartlyUnreadByALinkedTreeUsingTheBudgetIsReadOnceTheBudgetAllows(final Vertx vertx) {
    // the hostile tree, read level by level next to the honest one, lists labels that do not
    // exist, which still use up the budget
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree hostile = new SignedTree("hostile.example");
    final SignedTree honest = new SignedTree("honest.example");
    final List<String> missingLabels = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      missingLabels.add(label("missing" + i));
    }
    hostile.publishRoot(1, hostile.publishBranch(missingLabels), hostile.publishBranch(List.of()));
    final List<String> honestEnrs = takeEnrs(2);
    honest.publish(1, honestEnrs);
    final List<String> parentEnrs = takeEnrs(1);
    parent.publish(1, parentEnrs, hostile, honest);
    // the three roots, their six branches, the parent's ENR and links, and the 10 missing labels
    final DNSResolver resolver =
        new DNSResolver(vertx, parent.link(), 0, Optional.of("127.0.0.1:" + dns.port()), 22);

    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(parentEnrs));

    final List<String> newParentEnrs = takeEnrs(1);
    parent.publish(2, newParentEnrs, honest);
    assertThat(resolver.collectAll())
        .containsExactlyInAnyOrderElementsOf(records(newParentEnrs, honestEnrs));
  }

  @Test
  void enrWhoseLookupFailedIsReadOnTheNextRefresh(final Vertx vertx) {
    final SignedTree tree = new SignedTree("tree.example");
    final List<String> enrs = takeEnrs(2);
    final String failing = tree.publishLeaf(enrs.get(0));
    final String read = tree.publishLeaf(enrs.get(1));
    tree.publishRoot(1, tree.publishBranch(List.of(failing, read)), tree.publishBranch(List.of()));
    final String failingName = failing + ".tree.example";
    final DNSResolver resolver = resolver(vertx, tree);

    dns.removeTxtRecord(failingName);
    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(enrs.subList(1, 2)));

    // the root has not changed, but its ENRs are read again, all of them
    dns.addTxtRecord(failingName, enrs.get(0));
    assertThat(resolver.collectAll()).containsExactlyInAnyOrderElementsOf(records(enrs));
    assertThat(resolver.collectAll()).isEmpty();
  }

  @Test
  void olderRootOfATreeWhoseEnrsWereNotAllReadIsStillIgnored(final Vertx vertx) {
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree child = new SignedTree("child.example");
    final List<String> childEnrs = takeEnrs(2);
    final String failing = child.publishLeaf(childEnrs.get(0));
    child.publishRoot(
        5,
        child.publishBranch(List.of(failing, child.publishLeaf(childEnrs.get(1)))),
        child.publishBranch(List.of()));
    dns.removeTxtRecord(failing + ".child.example");
    final List<String> parentEnrs = takeEnrs(1);
    parent.publish(1, parentEnrs, child);
    final DNSResolver resolver = resolver(vertx, parent);
    assertThat(resolver.collectAll())
        .containsExactlyInAnyOrderElementsOf(records(parentEnrs, childEnrs.subList(1, 2)));

    // an older root of the child, validly signed
    child.publish(4, takeEnrs(1));
    assertThat(resolver.collectAll()).isEmpty();
  }

  @Test
  void invalidEnrRecordDoesNotMakeTheTreeBeReadAgain(final Vertx vertx) {
    // content that does not match its label is the publisher's error, and would be again
    final SignedTree tree = new SignedTree("tree.example");
    final List<String> enrs = takeEnrs(2);
    final String invalid = tree.publishLeaf(enrs.get(0));
    tree.publishRoot(
        1,
        tree.publishBranch(List.of(invalid, tree.publishLeaf(enrs.get(1)))),
        tree.publishBranch(List.of()));
    dns.addTxtRecord(invalid + ".tree.example", takeEnrs(1).getFirst());
    final DNSResolver resolver = resolver(vertx, tree);

    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(enrs.subList(1, 2)));
    assertThat(resolver.collectAll()).isEmpty();
  }

  @Test
  void refreshThatRunsOutOfTimeStopsAndItsUnreadEnrsAreReadLater(final Vertx vertx) {
    // the name server of the slow tree takes 5 s to answer each of its first labels, which do not
    // exist; its ENR comes after them
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree slow = new SignedTree("slow.example");
    final List<String> slowEnrLabels = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      slowEnrLabels.add(label("slow" + i));
    }
    final Set<String> slowNames =
        slowEnrLabels.stream().map(label -> label + ".slow.example").collect(Collectors.toSet());
    final List<String> slowEnrs = takeEnrs(1);
    slowEnrLabels.add(slow.publishLeaf(slowEnrs.getFirst()));
    slow.publishRoot(1, slow.publishBranch(slowEnrLabels), slow.publishBranch(List.of()));
    final List<String> parentEnrs = takeEnrs(1);
    parent.publish(1, parentEnrs, slow);
    final AtomicLong nanoTime = new AtomicLong();
    final AtomicInteger slowQueries = new AtomicInteger();
    dns.onQuery(
        name -> {
          if (slowNames.contains(name)) {
            slowQueries.incrementAndGet();
            nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(5));
          }
        });
    final DNSResolver resolver =
        new DNSResolver(
            vertx,
            parent.link(),
            0,
            Optional.of("127.0.0.1:" + dns.port()),
            DNSResolver.MAX_RECORDS_PER_REFRESH,
            Duration.ofSeconds(12),
            nanoTime::get);

    // the six slow labels would take 30 s: the refresh stops after the third
    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(parentEnrs));
    assertThat(slowQueries).hasValue(3);

    // the next refresh still reads a new root of the configured tree, and stops in time again
    final List<String> newParentEnrs = takeEnrs(1);
    parent.publish(2, newParentEnrs, slow);
    slowQueries.set(0);
    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(newParentEnrs));
    assertThat(slowQueries).hasValue(3);

    // once the slow tree answers quickly, its ENR is read, although its root has not changed
    dns.onQuery(name -> {});
    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(slowEnrs));
  }

  @Test
  void enrBelowTheLinkRootIsIgnored(final Vertx vertx) {
    final SignedTree tree = new SignedTree("tree.example");
    final List<String> enr = takeEnrs(1);
    tree.publishRoot(
        1,
        tree.publishBranch(List.of(tree.publishLeaf(enr.getFirst()))),
        tree.publishBranch(List.of(tree.publishLeaf(takeEnrs(1).getFirst()))));
    final DNSResolver resolver = resolver(vertx, tree);

    assertThat(resolver.collectAll()).containsExactlyElementsOf(records(enr));
    // the link subtree of the unchanged tree is read again, and still yields nothing
    assertThat(resolver.collectAll()).isEmpty();
  }

  @Test
  void linkBelowTheEnrRootIsNotFollowed(final Vertx vertx) {
    final SignedTree parent = new SignedTree("parent.example");
    final SignedTree child = new SignedTree("child.example");
    child.publish(1, takeEnrs(1));
    final List<String> parentEnr = takeEnrs(1);
    parent.publishRoot(
        1,
        parent.publishBranch(
            List.of(parent.publishLeaf(parentEnr.getFirst()), parent.publishLeaf(child.link()))),
        parent.publishBranch(List.of()));

    assertThat(resolver(vertx, parent).collectAll()).containsExactlyElementsOf(records(parentEnr));
  }

  private static String label(final String content) {
    return unpadded(
        Base32.encode(
            Hash.keccak256(Bytes.wrap(content.getBytes(StandardCharsets.UTF_8))).slice(0, 16)));
  }

  private static String linkRoot(final String root) {
    return Splitter.on(' ')
        .splitToStream(root)
        .filter(kv -> kv.startsWith("l="))
        .findFirst()
        .orElseThrow()
        .substring(2);
  }

  private DNSResolver resolver(final Vertx vertx, final SignedTree tree) {
    return new DNSResolver(vertx, tree.link(), 0, Optional.of("127.0.0.1:" + dns.port()));
  }

  private List<String> takeEnrs(final int count) {
    final List<String> taken = enrs.subList(nextEnr, nextEnr + count);
    nextEnr += count;
    return taken;
  }

  @SafeVarargs
  private static List<EthereumNodeRecord> records(final List<String>... enrLists) {
    final List<EthereumNodeRecord> records = new ArrayList<>();
    for (final List<String> enrList : enrLists) {
      for (final String enr : enrList) {
        records.add(((DNSEntry.ENRNode) DNSResolver.readDNSEntry(enr)).nodeRecord());
      }
    }
    return records;
  }

  /** A tree signed with its own key, published on the mock DNS server under its own domain. */
  private final class SignedTree {
    private final String domain;
    private final SECP256K1.KeyPair keyPair;

    private SignedTree(final String domain) {
      this(domain, SECP256K1.KeyPair.random());
    }

    private SignedTree(final String domain, final SECP256K1.KeyPair keyPair) {
      this.domain = domain;
      this.keyPair = keyPair;
    }

    /** The same tree, under a different spelling of its domain. */
    private SignedTree spelledAs(final String otherDomain) {
      return new SignedTree(otherDomain, keyPair);
    }

    private String link() {
      final byte[] compressedKey = keyPair.publicKey().asEcPoint().getEncoded(true);
      return "enrtree://" + unpadded(Base32.encodeBytes(compressedKey)) + "@" + domain;
    }

    /** Publishes the tree and returns its root record, so that it can be published again. */
    private String publish(final long seq, final List<String> treeEnrs, final SignedTree... links) {
      final List<String> enrLabels = new ArrayList<>();
      treeEnrs.forEach(enr -> enrLabels.add(publishLeaf(enr)));
      final List<String> linkLabels = new ArrayList<>();
      for (final SignedTree link : links) {
        linkLabels.add(publishLeaf(link.link()));
      }
      return publishRoot(seq, publishBranch(enrLabels), publishBranch(linkLabels));
    }

    private String publishRoot(final long seq, final String enrRoot, final String linkRoot) {
      final String signedContent =
          "enrtree-root:v1 e=" + enrRoot + " l=" + linkRoot + " seq=" + seq;
      final SECP256K1.Signature signature =
          SECP256K1.signHashed(
              Hash.keccak256(Bytes.wrap(signedContent.getBytes(StandardCharsets.UTF_8))), keyPair);
      final String root =
          signedContent + " sig=" + unpadded(Base64URLSafe.encode(signature.bytes()));
      dns.addTxtRecord(domain, root);
      return root;
    }

    private String publishBranch(final List<String> labels) {
      return publishLeaf("enrtree-branch:" + String.join(",", labels));
    }

    /** Publishes a record at the subdomain named after its hash, and returns that name. */
    private String publishLeaf(final String content) {
      final String label =
          unpadded(
              Base32.encode(
                  Hash.keccak256(Bytes.wrap(content.getBytes(StandardCharsets.UTF_8)))
                      .slice(0, 16)));
      dns.addTxtRecord(label + "." + domain, content);
      return label;
    }
  }

  private static String unpadded(final String encoded) {
    return encoded.replace("=", "");
  }
}
