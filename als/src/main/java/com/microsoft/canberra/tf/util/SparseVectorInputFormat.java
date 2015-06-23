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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hadoop MapReduce parser that reads lines of tab-separated id:frequency pairs into an array.
 * It is a wrapper around TextInputFormat.
 */
public final class SparseVectorInputFormat implements InputFormat<LongWritable, Document>, JobConfigurable {

  private static final Logger LOG = Logger.getLogger(SparseVectorInputFormat.class.getName());

  private final TextInputFormat textInputFormat = new TextInputFormat();

  @Override
  public RecordReader<LongWritable, Document> getRecordReader(
      final InputSplit inputSplit, final JobConf entries, final Reporter reporter) throws IOException {
    LOG.log(Level.FINEST, "Split {0} :: {1}", new Object[] { inputSplit, entries });
    return new SparseVectorRecordReader(
        this.textInputFormat.getRecordReader(inputSplit, entries, reporter));
  }

  public static void addInputPath(final JobConf jobConf, final Path path) {
    TextInputFormat.addInputPath(jobConf, path);
  }

  @Override
  public InputSplit[] getSplits(final JobConf entries, final int i) throws IOException {
    LOG.log(Level.FINEST, "getSplits {0} :: {1}", new Object[] { i, entries });
    return this.textInputFormat.getSplits(entries, i);
  }

  @Override
  public void configure(final JobConf entries) {
    this.textInputFormat.configure(entries);
  }
}
