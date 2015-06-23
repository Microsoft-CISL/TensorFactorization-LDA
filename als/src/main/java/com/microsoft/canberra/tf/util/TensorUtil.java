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

import org.jblas.DoubleMatrix;
import org.jblas.Singular;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class TensorUtil {

  public final static long RANDOM_SEED = 976180231;

  private final static double TOLERANCE = 1e-6;

  private static final Logger LOG = Logger.getLogger(TensorUtil.class.getName());

  public static DoubleMatrix gaussian(final int rows, final int cols) {
    return gaussian(rows, cols, RANDOM_SEED);
  }

  public static DoubleMatrix gaussian(final int rows, final int cols, final long seed) {

    final double[] data = new double[rows * cols];
    final Random rand = new Random(seed);

    for (int i = 0; i < data.length; ++i) {
      data[i] = rand.nextGaussian();
    }

    return new DoubleMatrix(rows, cols, data);
  }

  /**
   * Gram-Schmidt orthonormalization. Modifies matrix in place.
   *
   * @return reference to the (modified in place) input matrix.
   */
  public static DoubleMatrix orthogonalize(final DoubleMatrix A) {

    for (int j = 0; j < A.columns; ++j) {

      final DoubleMatrix Yj = A.getColumn(j);

      for (int i = 0; i < j; ++i) {
        final DoubleMatrix Yi = A.getColumn(i);
        Yj.subi(Yi.mul(Yi.dot(Yj)));
        Yj.subi(Yi.mul(Yi.dot(Yj)));
      }

      A.putColumn(j, Yj.divi(Yj.norm2()));
    }

    return A;
  }

  /**
   * Normalize matrix A in place.
   *
   * @param A A to normalize. (modified by the function!)
   * @param lambdas vector. (modified by the function!)
   */
  public static DoubleMatrix normalize(final DoubleMatrix A, final DoubleMatrix lambdas) {
    for (int i = 0; i < A.columns; ++i) {
      final DoubleMatrix col = A.getColumn(i);
      lambdas.put(i, col.norm2());
      A.putColumn(i, col.divi(lambdas.get(i)));
    }
    return A;
  }

  public static DoubleMatrix pinv(final DoubleMatrix A) {
    final DoubleMatrix[] usv = Singular.fullSVD(A);
    final DoubleMatrix s_inv = new DoubleMatrix(usv[1].length);
    for (int i = 0; i < usv[1].length; i++) {
      s_inv.put(i, Math.abs(usv[1].get(i)) < TOLERANCE ? 0.0 : 1.0 / usv[1].get(i));
    }
    return usv[2].mmul(DoubleMatrix.diag(s_inv)).mmul(usv[0].transpose());
  }

  public static DoubleMatrix multiKhatriRao(
      DoubleMatrix t, final int i, final DoubleMatrix C, final DoubleMatrix B, final double r) {

    final int k = C.columns;
    t = t.getRow(i).reshape(k, k);

    final DoubleMatrix out = DoubleMatrix.zeros(k);

    // LOG.log(Level.FINEST, "TensorUtil i = {0} out = {1}", new Object[] { i, out });

    for (int j = 0; j < C.columns; ++j) {
      out.put(j, out.get(j) + B.getColumn(j).transpose().mmul(t.mmul(C.getColumn(j))).get(0));
    }

    return out;
  }
}
