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
package com.microsoft.canberra.tf.util;

import com.microsoft.canberra.tf.task.Document;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Hadoop MapReduce parser that parses a line of tab-separated id:frequency pairs into a map.
 */
public final class SparseVectorRecordReader implements RecordReader<LongWritable, Document> {

  private static final Logger LOG = Logger.getLogger(SparseVectorRecordReader.class.getName());

  private final RecordReader<LongWritable, Text> textRecordReader;

  private final LongWritable offset;
  private final Text text;

  public SparseVectorRecordReader(final RecordReader<LongWritable, Text> textRecordReader) {
    this.textRecordReader = textRecordReader;
    this.offset = this.textRecordReader.createKey();
    this.text = this.textRecordReader.createValue();
  }

  @Override
  public boolean next(final LongWritable recordId, final Document data) throws IOException {

    if (!this.textRecordReader.next(this.offset, this.text)) {
      return false;
    }

    // LOG.log(Level.FINEST, "RecordReader: {0} :: {1}", new Object[] { this.offset, this.text });

    final String[] fields = this.text.toString().split("\\s+");

    if (fields.length <= 1) {
      return false;
    }

    // First element contains the unique document ID:
    recordId.set(Long.parseLong(fields[0]));

    data.clearTokens(recordId.get(), fields.length - 2);

    // Second element contains the number of unique items in the document:
    // assert(Integer.parseInt(fields[1]) == fields.length - 2);

    for (int i = 2; i < fields.length; ++i) {
      // Each element is colon-separated pair of integers, item_hash:item_frequency
      final String[] pair = fields[i].split(":", 2);
      data.add(Integer.valueOf(pair[0]), Double.valueOf(pair[1]));
    }

    // LOG.log(Level.FINEST, "RecordReader: {0} :: {1}", new Object[] { recordId, data });

    return true;
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public Document createValue() {
    return new Document();
  }

  @Override
  public long getPos() throws IOException {
    return this.textRecordReader.getPos();
  }

  @Override
  public void close() throws IOException {
    this.textRecordReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return this.textRecordReader.getProgress();
  }
}
