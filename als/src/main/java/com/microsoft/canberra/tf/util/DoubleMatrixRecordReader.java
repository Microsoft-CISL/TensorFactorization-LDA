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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.jblas.DoubleMatrix;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hadoop MapReduce record reader that parses one line of tab-separated text
 * into jBLAS DoubleMatrix vector.
 */
public final class DoubleMatrixRecordReader implements RecordReader<IntWritable, DoubleMatrix> {

  private static final Logger LOG = Logger.getLogger(DoubleMatrixRecordReader.class.getName());

  private final RecordReader<LongWritable, Text> textRecordReader;
  private final LongWritable offset;
  private final Text text;

  public DoubleMatrixRecordReader(final RecordReader<LongWritable, Text> textRecordReader) {
    this.textRecordReader = textRecordReader;
    this.offset = this.textRecordReader.createKey();
    this.text = this.textRecordReader.createValue();
  }

  @Override
  public boolean next(final IntWritable rowId, final DoubleMatrix matrixRow) throws IOException {

    if (!this.textRecordReader.next(this.offset, this.text)) {
      return false;
    }

    LOG.log(Level.FINEST, "RecordReader: {0} :: {1}", new Object[] { this.offset, this.text });

    final String[] fields = this.text.toString().split("\\s+");

    if (fields.length <= 1) {
      return false;
    }

    rowId.set(Integer.parseInt(fields[0]));
    matrixRow.resize(fields.length - 1, 1);

    for (int i = 1; i < fields.length; ++i) {
      matrixRow.put(i - 1, Double.parseDouble(fields[i]));
    }

    return true;
  }

  @Override
  public IntWritable createKey() {
    return new IntWritable();
  }

  @Override
  public DoubleMatrix createValue() {
    return new DoubleMatrix();
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
