/*
 * Copyright (C) 2013  Ohm Data
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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */
/*
 * Copyright 2012 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package c5db.client.queue;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * <ul>
 * <li>Lock free, observing single writer principal.
 * <li>Replacing the long fields with AtomicLong and using lazySet instead of
 * volatile assignment.
 * <li>Using the power of 2 mask, forcing the capacity to next power of 2.
 * <li>Adding head and tail cache fields. Avoiding redundant volatile reads.
 * <li>Padding head/tail AtomicLong fields. Avoiding false sharing.
 * <li>Padding head/tail cache fields. Avoiding false sharing.
 * </ul>
 */
public final class WickedQueue<E> implements Queue<E> {
  private final int capacity;
  private final int mask;
  private final E[] buffer;

  private final AtomicLong tail = new c5db.client.queue.PaddedAtomicLong();
  private final AtomicLong head = new c5db.client.queue.PaddedAtomicLong();
  private final PaddedLong tailCache = new PaddedLong();
  private final PaddedLong headCache = new PaddedLong();

  @SuppressWarnings("unchecked")
  public WickedQueue() {
    this.capacity = findNextPositivePowerOfTwo();
    mask = this.capacity - 1;
    buffer = (E[]) new Object[this.capacity];
  }

  private static int findNextPositivePowerOfTwo() {
    return 1 << (32 - Integer.numberOfLeadingZeros(c5db.client.C5Constants.MAX_CACHE_SZ - 1));
  }

  public boolean add(final E e) {
    if (offer(e)) {
      return true;
    }

    throw new IllegalStateException("Queue is full");
  }

  public boolean offer(final E e) {
    if (null == e) {
      throw new NullPointerException("Null is not a valid element");
    }

    final long currentTail = tail.get();
    final long wrapPoint = currentTail - capacity;
    if (headCache.value <= wrapPoint) {
      headCache.value = head.get();
      if (headCache.value <= wrapPoint) {
        return false;
      }
    }

    buffer[(int) currentTail & mask] = e;
    tail.lazySet(currentTail + 1);

    return true;
  }

  public E poll() {
    final long currentHead = head.get();
    if (currentHead >= tailCache.value) {
      tailCache.value = tail.get();
      if (currentHead >= tailCache.value) {
        return null;
      }
    }

    final int index = (int) currentHead & mask;
    final E e = buffer[index];
    buffer[index] = null;
    head.lazySet(currentHead + 1);

    return e;
  }

  public E remove() {
    final E e = poll();
    if (null == e) {
      throw new NoSuchElementException("Queue is empty");
    }

    return e;
  }

  public E element() {
    final E e = peek();
    if (null == e) {
      throw new NoSuchElementException("Queue is empty");
    }

    return e;
  }

  public E peek() {
    return buffer[(int) head.get() & mask];
  }

  public int size() {
    return (int) (tail.get() - head.get());
  }

  public boolean isEmpty() {
    return tail.get() == head.get();
  }

  public boolean contains(final Object o) {
    if (null == o) {
      return false;
    }

    for (long i = head.get(), limit = tail.get(); i < limit; i++) {
      final E e = buffer[(int) i & mask];
      if (o.equals(e)) {
        return true;
      }
    }

    return false;
  }

  public Iterator<E> iterator() {
    throw new UnsupportedOperationException();
  }

  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  public <T> T[] toArray(final T[] a) {
    throw new UnsupportedOperationException();
  }

  public boolean remove(final Object o) {
    throw new UnsupportedOperationException();
  }

  public boolean containsAll(final Collection<?> c) {
    for (final Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }

    return true;
  }

  public boolean addAll(final Collection<? extends E> c) {
    addAll(c.stream().collect(Collectors.toList()));
    return true;
  }

  public boolean removeAll(final Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  public boolean retainAll(final Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  public void clear() {
    Object value;
    do {
      value = poll();
    } while (null != value);
  }

  public static class PaddedLong {
    public long value = 0, p1, p2, p3, p4, p5, p6;
  }
}