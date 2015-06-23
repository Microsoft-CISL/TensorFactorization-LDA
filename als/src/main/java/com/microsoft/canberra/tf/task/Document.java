/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014-2015 Microsoft
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.microsoft.canberra.tf.task;

import java.util.ArrayList;
import java.util.Iterator;

public final class Document implements Iterable<Document.Token> {

  public static final class Token {

    public final int id;
    public final double count;

    private Token(final int id, final double count) {
      this.id = id;
      this.count = count;
    }

    @Override
    public String toString() {
      return this.id + ":" + this.count;
    }
  }

  private long id;
  private final ArrayList<Token> tokens;

  /**
   * Default constructor required for Hadoop InputFormat
   */
  public Document() {
    this(-1, new ArrayList<Token>());
  }

  public Document(final long id, final ArrayList<Token> tokens) {
    this.id = id;
    this.tokens = tokens;
  }

  /**
   * Clear the tokens array and prepare it for population with the new data.
   * This function is required for Hadoop InputFormat.
   */
  public void clearTokens(final long id, final int capacity) {
    this.id = id;
    this.tokens.clear();
    this.tokens.ensureCapacity(capacity);
  }

  public void add(final int id, final double count) {
    this.tokens.add(new Token(id, count));
  }

  public long getId() {
    return this.id;
  }

  public int size() {
    return this.tokens.size();
  }

  @Override
  public Iterator<Token> iterator() {
    return this.tokens.iterator();
  }

  @Override
  public String toString() {
    return this.id + " :: " + this.tokens;
  }
}
