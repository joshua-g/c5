/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package c5db.log;

import c5db.C5CommonTestUtil;
import c5db.util.KeySerializingExecutor;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static c5db.FutureMatchers.resultsIn;
import static c5db.log.EncodedSequentialLog.LogEntryNotInSequence;
import static c5db.log.LogTestUtil.emptyEntryList;
import static c5db.log.LogTestUtil.makeEntry;
import static c5db.log.LogTestUtil.makeSingleEntryList;
import static c5db.log.LogTestUtil.seqNum;
import static c5db.log.LogTestUtil.term;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QuorumDelegatingLogTest {
  private static Path testDirectory;
  private LogFileService logPersistenceService;
  private OLog log;
  private Random deterministicDataSequence = null; // Used as a pre-seeded data source for at least one test
  private final String quorumId = "quorumId";

  @BeforeClass
  public static void setTestDirectory() {
    if (testDirectory == null) {
      testDirectory = (new C5CommonTestUtil()).getDataTestDir("olog");
    }
  }

  @Before
  public final void setUp() throws Exception {
    deterministicDataSequence = new Random(2231983);
    testSequenceNumber = aSeqNum();
    logPersistenceService = new LogFileService(testDirectory);
    logPersistenceService.moveLogsToArchive();

    log = new QuorumDelegatingLog(
        logPersistenceService,
        new KeySerializingExecutor(MoreExecutors.sameThreadExecutor()),
        NavigableMapTermOracle::new,
        InMemoryPersistenceNavigator::new);
  }

  @After
  public final void tearDown() throws Exception {
    log.close();
    logPersistenceService.moveLogsToArchive();
    deterministicDataSequence = null;
  }

  @Test(expected = Exception.class, timeout = 1000)
  public void throwsExceptionFromGetLogEntryMethodWhenTheLogIsEmpty() throws Exception {
    log.getLogEntry(1, quorumId).get();
  }

  @Test(expected = Exception.class, timeout = 1000)
  public void throwsExceptionFromGetLogEntriesMethodWhenTheLogIsEmpty() throws Exception {
    log.getLogEntries(1, 2, quorumId).get();
  }

  @Test
  public void canLogAndRetrieveASingleEntry() throws Exception {
    OLogEntry entryToLog = anOLogEntry();
    long seqNum = entryToLog.getSeqNum();

    log.logEntry(Lists.newArrayList(entryToLog), quorumId);

    assertThat(log.getLogEntry(seqNum, quorumId),
        resultsIn(equalTo(entryToLog)));
  }

  @Test
  public void retrievesListsOfEntriesItHasLogged() throws Exception {
    List<OLogEntry> entries = someConsecutiveEntries(1, 5);

    log.logEntry(entries, quorumId);

    assertThat(log.getLogEntries(1, 5, quorumId),
        resultsIn(equalTo(entries)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwsAnExceptionWhenGettingEntriesIfEndIsLessThanStart() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    log.getLogEntries(3, 2, quorumId);
  }

  @Test
  public void returnsAnEmptyListWhenGettingEntriesIfStartEqualsEnd() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    assertThat(log.getLogEntries(3, 3, quorumId), resultsIn(equalTo(emptyEntryList())));
  }

  @Test
  public void logsAndRetrievesDifferentQuorumsWithTheSameSequenceNumbers() throws Exception {
    String quorumA = "A";
    String quorumB = "B";

    List<OLogEntry> entriesA = someConsecutiveEntries(1, 5);
    List<OLogEntry> entriesB = someConsecutiveEntries(1, 5);

    log.logEntry(entriesA, quorumA);
    log.logEntry(entriesB, quorumB);

    assertThat(log.getLogEntries(1, 5, quorumA), resultsIn(equalTo(entriesA)));
    assertThat(log.getLogEntries(1, 5, quorumB), resultsIn(equalTo(entriesB)));
  }

  @Test
  public void retrievesEntriesFromTheMiddleOfTheLog() throws Exception {
    List<OLogEntry> entries = someConsecutiveEntries(1, 10);

    log.logEntry(entries, quorumId);

    assertThat(log.getLogEntries(4, 6, quorumId), resultsIn(equalTo(entries.subList(3, 5))));
  }

  @Test
  public void truncatesEntriesFromTheEndOfTheLog() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    log.truncateLog(3, quorumId);
    log.logEntry(someConsecutiveEntries(3, 5), quorumId);
  }

  @Test(expected = LogEntryNotInSequence.class)
  public void throwsAnExceptionIfAttemptingToLogEntriesWithASequenceGap() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 3), quorumId);
    log.logEntry(someConsecutiveEntries(4, 5), quorumId);
  }

  @Test(expected = LogEntryNotInSequence.class)
  public void throwsAnExceptionIfAttemptingToLogEntriesWithNonascendingSequenceNumber() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 2), quorumId);
    log.logEntry(someConsecutiveEntries(1, 2), quorumId);
  }

  @Test(expected = Exception.class, timeout = 1000)
  public void throwsAnExceptionIfTryingToRetrieveAnEntryThatIsntInTheLog() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    log.truncateLog(3, quorumId);
    log.getLogEntry(4, quorumId).get();
  }

  @Test(expected = Exception.class, timeout = 1000)
  public void throwsAnExceptionIfTryingToRetrieveEntriesAndAtLeastOneIsntInTheLog() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    log.truncateLog(3, quorumId);
    log.getLogEntries(2, 4, quorumId).get();
  }

  @Test
  public void maintainsCorrectSequenceAfterATruncation() {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    log.truncateLog(3, quorumId);
    log.logEntry(someConsecutiveEntries(3, 6), quorumId);
  }

  @Test
  public void storesAndRetrievesElectionTermForEntriesItHasLogged() {
    log.logEntry(makeSingleEntryList(nextSeqNum(), term(1), someData()), quorumId);
    log.logEntry(makeSingleEntryList(nextSeqNum(), term(2), someData()), quorumId);

    assertThat(log.getLogTerm(lastSeqNum(), quorumId), is(equalTo(term(2))));
    assertThat(log.getLogTerm(lastSeqNum() - 1, quorumId), is(equalTo(term(1))));
  }

  @Test
  public void retrievesCorrectElectionTermAfterATruncation() {
    log.logEntry(makeSingleEntryList(nextSeqNum(), term(1), someData()), quorumId);
    log.logEntry(makeSingleEntryList(nextSeqNum(), term(2), someData()), quorumId);
    log.truncateLog(lastSeqNum(), quorumId);
    log.logEntry(makeSingleEntryList(lastSeqNum(), term(3), someData()), quorumId);

    assertThat(log.getLogTerm(lastSeqNum(), quorumId), is(equalTo(term(3))));
  }

  @Test
  public void returnsResultsFromGetLogEntriesThatTakeIntoAccountMutationsRequestedEarlier() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 10), quorumId);
    log.truncateLog(5, quorumId);
    List<OLogEntry> replacementEntries = someConsecutiveEntries(5, 10);
    log.logEntry(replacementEntries, quorumId);

    assertThat(log.getLogEntries(5, 10, quorumId), resultsIn(equalTo(replacementEntries)));
  }

  @Test
  public void canReturnTheLastSequenceNumberAndTermInTheLog() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 10), quorumId);
    log.logEntry(makeSingleEntryList(seqNum(10), term(100), someData()), quorumId);

    assertThat(log.getLastTerm(quorumId), resultsIn(equalTo(100L)));
    assertThat(log.getLastSeqNum(quorumId), resultsIn(equalTo(10L)));
  }

  @Test(timeout = 1000)
  public void correctlyReturnsLastSequenceNumberAndTermAfterATruncation() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    long term = log.getLastTerm(quorumId).get() + 1;

    log.logEntry(makeSingleEntryList(seqNum(5), term, someData()), quorumId);
    log.logEntry(makeSingleEntryList(seqNum(6), term + 1, someData()), quorumId);
    log.truncateLog(6, quorumId);

    assertThat(log.getLastSeqNum(quorumId), resultsIn(equalTo(5L)));
    assertThat(log.getLastTerm(quorumId), resultsIn(equalTo(term)));
  }

  @Test
  public void returnsZeroForLastSequenceNumberAndTermWhenThatQuorumHasNoEntries() throws Exception {
    assertThat(log.getLastTerm(quorumId), resultsIn(equalTo(0L)));
    assertThat(log.getLastSeqNum(quorumId), resultsIn(equalTo(0L)));
  }

  /**
   * Private methods
   */

  private long anElectionTerm() {
    return Math.abs(deterministicDataSequence.nextLong());
  }

  private long aSeqNum() {
    return Math.abs(deterministicDataSequence.nextLong());
  }

  private long testSequenceNumber;

  private long nextSeqNum() {
    testSequenceNumber++;
    return testSequenceNumber;
  }

  private long lastSeqNum() {
    return testSequenceNumber;
  }

  private ByteBuffer someData() {
    byte[] bytes = new byte[10];
    deterministicDataSequence.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  private OLogEntry anOLogEntry() {
    return makeEntry(aSeqNum(), anElectionTerm(), someData());
  }

  /**
   * Create and return (end - start) entries, in ascending sequence number order, from start inclusive,
   * to end exclusive.
   */
  private List<OLogEntry> someConsecutiveEntries(int start, int end) {
    List<OLogEntry> entries = Lists.newArrayList();
    for (int i = start; i < end; i++) {
      entries.add(makeEntry(i, IntMath.divide(i + 1, 2, RoundingMode.CEILING), someData()));
    }
    return entries;
  }
}
