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

import org.apache.reef.io.network.group.api.operators.Reduce;
import org.jblas.DoubleMatrix;
import javax.inject.Inject;

public class MatrixSumReducer implements Reduce.ReduceFunction<DoubleMatrix[]> {

  private static final DoubleMatrix[] EMPTY = new DoubleMatrix[0];

  @Inject
  public MatrixSumReducer() {
  }

  @Override
  public synchronized DoubleMatrix[] apply(final Iterable<DoubleMatrix[]> iter) {

    DoubleMatrix[] res = EMPTY;

    for (final DoubleMatrix[] matrices : iter) {
      if (res == EMPTY) {
        res = new DoubleMatrix[matrices.length];
        for (int i = 0; i < res.length; ++i) {
          res[i] = matrices[i].dup();
        }
      } else {
        for (int i = 0; i < res.length; ++i) {
          res[i].addi(matrices[i]);
        }
      }
    }

    return res;
  }
}
