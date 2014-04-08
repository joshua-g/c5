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

import com.google.common.collect.Lists;
import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static c5db.log.EncodedSequentialLog.Codec;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogTestUtil.makeEntry;
import static c5db.log.LogTestUtil.seqNum;
import static c5db.log.LogTestUtil.term;
import static org.hamcrest.CoreMatchers.equalTo;

@SuppressWarnings("unchecked")
public class EncodedSequentialLogTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();

  private final BytePersistence persistence = context.mock(BytePersistence.class);
  private final Codec<OLogEntry> codec = context.mock(Codec.class);
  private final PersistenceNavigator navigator = context.mock(PersistenceNavigator.class);

  private final SequentialLog<OLogEntry> log = new EncodedSequentialLog<>(persistence, codec, navigator);

  @Test
  public void writesToTheSuppliedPersistenceObjectUsingTheSuppliedCodec() throws Exception {
    OLogEntry entry = makeEntry(seqNum(1), term(2), "data");

    context.checking(new Expectations() {{
      oneOf(codec).encode(with(equalTo(entry)));
      atLeast(1).of(persistence).append(with(any(ByteBuffer[].class)));
      ignoring(navigator);
    }});

    log.append(Lists.newArrayList(entry));
  }

  @Test
  public void notifiesTheNavigatorOnceForEveryEntryWritten() throws Exception {
    context.checking(new Expectations() {{
      ignoring(codec);
      ignoring(persistence);
      exactly(5).of(navigator).notify(with(any(Long.class)));
    }});

    log.append(someConsecutiveEntries(1, 6));
  }

  @Test
  public void readsEntriesFromTheSuppliedPersistenceObjectUsingTheSuppliedCodec() throws Exception {
    context.checking(new Expectations() {{
      codecWillReturnEntrySequence(codec, someConsecutiveEntries(1, 6));
      allowing(persistence).getReader();
      allowing(navigator).getStream(1);
      will(returnValue(aMockInputStream()));
    }});

    log.subSequence(1, 6);
  }

  @Test
  public void processesTruncationRequestsByDelegatingThemToTheSuppliedPersistence() throws Exception {
    context.checking(new Expectations() {{
      oneOf(navigator).getAddressOfEntry(seqNum(7));
      will(returnValue(100L));
      oneOf(persistence).truncate(100);
    }});

    log.truncate(seqNum(7));
  }

  @Test
  public void canReturnTheLastEntryInTheLog() throws Exception {
    context.checking(new Expectations() {{
      oneOf(navigator).getLastEntry();
    }});

    log.getLastEntry();
  }

  private List<OLogEntry> someConsecutiveEntries(int start, int end) {
    List<OLogEntry> entries = Lists.newArrayList();
    for (int i = start; i < end; i++) {
      entries.add(makeEntry(seqNum(i), term(7), "data"));
    }
    return entries;
  }

  private InputStream aMockInputStream() {
    return new InputStream() {
      @Override
      public int read() throws IOException {
        return 0;
      }
    };
  }

  private void codecWillReturnEntrySequence(Codec<OLogEntry> codec, List<OLogEntry> entries) throws Exception {
    Sequence seq = context.sequence("Codec#decode method call sequence");
    context.checking(new Expectations() {{
      for (OLogEntry e : entries) {
        oneOf(codec).decode(with(any(InputStream.class)));
        will(returnValue(e));
        inSequence(seq);
      }
    }});
  }
}
