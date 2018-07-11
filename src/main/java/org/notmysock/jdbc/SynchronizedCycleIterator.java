package org.notmysock.jdbc;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class SynchronizedCycleIterator<E> implements Iterator<E> {

  private final E[] items;
  private final AtomicInteger index = new AtomicInteger(0);
  
  public SynchronizedCycleIterator(E[] items) {
    this.items = items;
  }
  
  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public E next() {
    return items[this.index.getAndIncrement() % items.length];
  }

}
