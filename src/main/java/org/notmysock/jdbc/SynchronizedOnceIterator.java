package org.notmysock.jdbc;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class SynchronizedOnceIterator<E> implements Iterator<E> {

  private final E[] items;
  private final AtomicInteger index = new AtomicInteger(0);
  
  public SynchronizedOnceIterator(E[] items) {
    this.items = items;
  }
  
  /** 
   * this is not enough to continue the loop, due to thread-safety
   * the next item might be NULL due to two threads both thinking there
   * as hasNext for the last value
   * @return
   */
  @Override
  public boolean hasNext() {
    if (this.index.get()+1 >= items.length) {
      return false;
    }
    return true;
  }

  @Override
  public E next() {
    int n = this.index.getAndIncrement();
    if (n >= items.length) {
      return null;
    }
    return items[n];
  }
}
