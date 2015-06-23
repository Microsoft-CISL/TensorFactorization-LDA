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
package com.microsoft.canberra.tf.task.als;

import com.microsoft.canberra.tf.Launch;
import com.microsoft.canberra.tf.task.TaskEnvironment;
import com.microsoft.canberra.tf.taskgroup.TaskGroupDescriptor;
import com.microsoft.canberra.tf.util.TensorUtil;

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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@TaskSide
public class AlsMasterTask implements Task {

  static final int[][] ABC_ORDER = {{0, 2, 1}, {1, 0, 2}, {2, 1, 0}};

  private static final long SEED_B = 1412218460;
  private static final long SEED_C = 2048512343;

  private static final TaskGroupDescriptor DESCRIPTOR = AlsTaskGroupDescriptor.INSTANCE;

  private static final Logger LOG = Logger.getLogger(AlsMasterTask.class.getName());

  private final int dimK;
  private final double tolerance;
  private final int maxIterations;
  private final TaskEnvironment env;

  private final Broadcast.Sender<DoubleMatrix[]> modelSender;
  private final Reduce.Receiver<ArrayList<AbstractMap.SimpleEntry<Integer, DoubleMatrix>>> resultReceiver;

  @Inject
  public AlsMasterTask(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final @Parameter(Launch.DimK.class) int dimK,
      final @Parameter(Launch.Tolerance.class) Double tolerance,
      final @Parameter(Launch.MaxIterations.class) Integer maxIterations,
      final GroupCommClient groupCommClient,
      final TaskEnvironment env) {

    this.dimK = dimK;
    this.tolerance = tolerance;
    this.maxIterations = maxIterations;
    this.env = env;

    final CommunicationGroupClient commGroup =
        groupCommClient.getCommunicationGroup(DESCRIPTOR.getCommGroupIdClass());

    this.modelSender = commGroup.getBroadcastSender(DESCRIPTOR.getBroadcastIdClass());
    this.resultReceiver = commGroup.getReduceReceiver(DESCRIPTOR.getReduceIdClass());

    LOG.log(Level.FINEST, "AlsMasterTask {0} created", taskId);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    LOG.log(Level.FINEST, "AlsMasterTask started: max {0} iterations", this.maxIterations);

    final DoubleMatrix[] ABC = new DoubleMatrix[] {
        DoubleMatrix.zeros(this.dimK, this.dimK),
        TensorUtil.orthogonalize(TensorUtil.gaussian(this.dimK, this.dimK, SEED_B)),
        TensorUtil.orthogonalize(TensorUtil.gaussian(this.dimK, this.dimK, SEED_C))
    };

    LOG.log(Level.FINEST, "AlsMasterTask started: B = {0}", ABC[1]);
    LOG.log(Level.FINEST, "AlsMasterTask started: C = {0}", ABC[2]);

    final DoubleMatrix prev = DoubleMatrix.EMPTY;

    final DoubleMatrix[] lambdas = new DoubleMatrix[] {
        new DoubleMatrix(this.dimK),
        new DoubleMatrix(this.dimK),
        new DoubleMatrix(this.dimK)
    };

    final DoubleMatrix[] toSend = new DoubleMatrix[] {
        DoubleMatrix.scalar(0),
        ABC[ABC_ORDER[0][1]],
        ABC[ABC_ORDER[0][2]]
    };

    int mode = 2;
    int iter = 0;
    for (; iter < this.maxIterations || this.maxIterations <= 0; ) {

      mode = ++mode % 3;
      final int[] modeIdx = ABC_ORDER[mode];
      final int i = modeIdx[0];

      if (mode == 0) {
        ++iter;
        if (isConverged(prev, ABC[i])) {
          break;
        }
        prev.copy(ABC[i]);
      }

      toSend[0].put(0, mode);
      toSend[1] = ABC[modeIdx[1]];

      this.modelSender.send(toSend);

      for (final Map.Entry<Integer, DoubleMatrix> row : this.resultReceiver.reduce()) {
        ABC[i].putRow(row.getKey(), row.getValue());
      }

      TensorUtil.normalize(ABC[i], lambdas[i]);
      if (mode != 0) {
        TensorUtil.orthogonalize(ABC[i]);
      }

      toSend[2] = null;

      LOG.log(Level.FINEST,
          "AlsMasterTask iteration {0} mode {1} = {2}; lambdas = {3}",
          new Object[] { iter, "ABC".charAt(mode), ABC[i], lambdas[i] });
    }

    // Send empty array to signal the end of the job:
    this.modelSender.send(new DoubleMatrix[] {});

    for (int i = 0; i < this.dimK; ++i) {

      final int[] a = new int[3];
      for (int j = 0; j < 3; ++j) {
        a[j] = (int) Math.signum(ABC[j].get(0, i));
      }

      if (a[0] != a[1] || a[0] != a[2]) {
        if (a[0] == a[2]) {
          ABC[0].putColumn(i, ABC[1].getColumn(i));
        } else if (a[0] == a[1]) {
          ABC[0].putColumn(i, ABC[2].getColumn(i));
        }
      }
    }

    this.modelSender.send(new DoubleMatrix[] { ABC[0] });

    this.env.setA(ABC[0])
            .setLambda(lambdas[0]);

    LOG.log(Level.FINE, "AlsMasterTask complete after {0} iterations", iter);

    LOG.log(Level.FINEST, "AlsMasterTask complete. sign-normalized A = {0}", ABC[0]);
    // LOG.log(Level.FINEST, "AlsMasterTask complete. B = {0}", ABC[1]);
    // LOG.log(Level.FINEST, "AlsMasterTask complete. C = {0}", ABC[2]);

    LOG.log(Level.FINEST, "AlsMasterTask complete. A lambdas = {0}", lambdas[0]);
    // LOG.log(Level.FINEST, "AlsMasterTask complete. B lambdas = {0}", lambdas[1]);
    // LOG.log(Level.FINEST, "AlsMasterTask complete. C lambdas = {0}", lambdas[2]);

    return null;
  }

  /**
   * Check for convergence. Do that only on pass A of ALS, and compare matrix A only.
   *
   * @param oldA new version of matrix A.
   * @param newA current version of matrix A.
   * @return true if converged.
   */
  private boolean isConverged(final DoubleMatrix oldA, final DoubleMatrix newA) {

    if (oldA == null || oldA.isEmpty()) {
      return false;
    }

    final double delta = newA.sub(oldA).norm2() / newA.norm2();

    // LOG.log(Level.FINEST, "Check for convergence: delta: {0} tolerance: {1}",
    //     new String[] { "" + delta, "" + this.tolerance });

    return delta < this.tolerance;
  }
}
