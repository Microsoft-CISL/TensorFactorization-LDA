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
package com.microsoft.canberra.tf.task.m3;

import com.microsoft.canberra.tf.taskgroup.TaskGroupDescriptor;
import com.microsoft.canberra.tf.Launch;
import com.microsoft.canberra.tf.SharedContextTaskGroup;
import com.microsoft.canberra.tf.task.TaskEnvironment;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.task.Task;
import org.apache.reef.tang.annotations.Parameter;

import org.jblas.DoubleMatrix;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Compute part of the M3 moments tensor.
 */
@TaskSide
public final class M3Task implements Task {

  private static final TaskGroupDescriptor DESCRIPTOR = M3TaskGroupDescriptor.INSTANCE;

  private static final Logger LOG = Logger.getLogger(M3Task.class.getName());

  private final double alpha0;
  private final int dimK;
  private final int startK;
  private final int endK;

  private final TaskEnvironment env;

  private final Broadcast.Receiver<DoubleMatrix> modelReceiver;
  private final Reduce.Sender<DoubleMatrix[]> resultSender;

  @Inject
  public M3Task(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final @Parameter(Launch.Alpha0.class) double alpha0,
      final @Parameter(Launch.DimK.class) int dimK,
      final @Parameter(SharedContextTaskGroup.StartK.class) int startK,
      final @Parameter(SharedContextTaskGroup.EndK.class) int endK,
      final GroupCommClient groupCommClient,
      final TaskEnvironment env) {

    final CommunicationGroupClient commGroup =
        groupCommClient.getCommunicationGroup(DESCRIPTOR.getCommGroupIdClass());

    this.alpha0 = alpha0;
    this.dimK = dimK;
    this.startK = startK;
    this.endK = endK;
    this.env = env;

    this.modelReceiver = commGroup.getBroadcastReceiver(DESCRIPTOR.getBroadcastIdClass());
    this.resultSender = commGroup.getReduceSender(DESCRIPTOR.getReduceIdClass());

    LOG.log(Level.FINEST, "M3Task {0} created: k = {1} range = {2}..{3}",
        new Object[] { taskId, dimK, startK, endK });
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    final int examples = this.env.getExamples();

    LOG.log(Level.FINEST, "M3Task started: {0} examples", examples);

    final DoubleMatrix wcSlice = this.env.getWc();
    final DoubleMatrix counts = this.env.getCounts();

    // LOG.log(Level.FINEST, "M3Task wc = {0}", wcSlice);
    // LOG.log(Level.FINEST, "M3Task counts = {0}", counts);

    final DoubleMatrix m1 = this.modelReceiver.receive();

    LOG.log(Level.FINEST, "M3Task M1 = {0}", m1);

    DoubleMatrix t = DoubleMatrix.zeros(this.dimK, this.dimK * this.dimK);

    double validExamples = 0;
    for (int n = 0; n < examples; ++n) {

      final double len = counts.get(n);
      final DoubleMatrix wc = wcSlice.getRow(n);

      if (len < 3) {
        continue;
      }

      ++validExamples;
      final double scale2fac = this.alpha0 * (this.alpha0 + 1) / (2. * len * (len - 1));
      final double scale3fac = (this.alpha0 + 1) * (this.alpha0 + 2) / (2. * len * (len - 1) * (len - 2));

      for (int i = 0; i < this.dimK; ++i) {

        final int iSlice = this.dimK * i;

        for (int j = 0; j < this.dimK; ++j) {

          final int jSlice = this.dimK * j;

          for (int k = 0; k < this.dimK; ++k) {
            t.put(i, jSlice + k, t.get(i, jSlice + k)
                // Topic shift scale3fac 1st term
                + scale3fac * wc.get(i) * wc.get(j) * wc.get(k)
                // Dirichlet 2nd order term
                - scale2fac * m1.get(i) * wc.get(j) * wc.get(k)
                - scale2fac * wc.get(i) * m1.get(j) * wc.get(k)
                - scale2fac * wc.get(i) * wc.get(j) * m1.get(k));
          }

          // Topic shift 2..4th terms
          t.put(i, iSlice + j, t.get(i, iSlice + j) - scale3fac * wc.get(i) * wc.get(j) + scale2fac * wc.get(i) * m1.get(j));
          t.put(i, jSlice + i, t.get(i, jSlice + i) - scale3fac * wc.get(i) * wc.get(j) + scale2fac * wc.get(i) * m1.get(j));
          t.put(i, jSlice + j, t.get(i, jSlice + j) - scale3fac * wc.get(i) * wc.get(j) + scale2fac * m1.get(i) * wc.get(j));

          // Topic shift scale3fac 5th term
          t.put(i, iSlice + i, t.get(i, iSlice + i) + 2.0 * scale3fac * wc.get(i));
        }
      }
    }

    LOG.log(Level.FINEST, "M3Task send Ta[] slice: {0} valid examples", validExamples);

    this.resultSender.send(new DoubleMatrix[] { DoubleMatrix.scalar(validExamples), t });

    {
      final int tRows[] = new int[this.endK - this.startK];
      for (int i = 0; i < tRows.length; ++i) {
        tRows[i] = this.startK + i;
      }

      t = this.modelReceiver.receive().getRows(tRows);
    }

    final double alpha0sq = this.alpha0 * this.alpha0;
    for (int i = this.startK; i < this.endK; ++i) {
      final int offset = i - this.startK;
      for (int j = 0; j < this.dimK; ++j) {
        final int jSlice = this.dimK * j;
        for (int k = 0; k < this.dimK; ++k) {
          t.put(offset, jSlice + k, t.get(offset, jSlice + k)
              + alpha0sq * m1.get(i) * m1.get(j) * m1.get(k));
        }
      }
    }

    this.env.clearM1()
            .clearWc()
            .clearCounts()
            .setT(t);

    LOG.log(Level.FINEST, "M3Task complete");

    return null;
  }
}
