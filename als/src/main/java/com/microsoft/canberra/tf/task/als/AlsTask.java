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
import com.microsoft.canberra.tf.SharedContextTaskGroup;
import com.microsoft.canberra.tf.taskgroup.TaskGroupDescriptor;
import com.microsoft.canberra.tf.task.TaskEnvironment;
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
import java.util.logging.Level;
import java.util.logging.Logger;

@TaskSide
public class AlsTask implements Task {

  private static final TaskGroupDescriptor DESCRIPTOR = AlsTaskGroupDescriptor.INSTANCE;

  private static final Logger LOG = Logger.getLogger(AlsTask.class.getName());

  private final int startK;
  private final int endK;
  private final int dimK;
  private final double rho;

  private final TaskEnvironment env;

  private final Broadcast.Receiver<DoubleMatrix[]> modelReceiver;
  private final Reduce.Sender<ArrayList<AbstractMap.SimpleEntry<Integer, DoubleMatrix>>> resultSender;

  @Inject
  public AlsTask(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final @Parameter(SharedContextTaskGroup.StartK.class) int startK,
      final @Parameter(SharedContextTaskGroup.EndK.class) int endK,
      final @Parameter(Launch.DimK.class) int dimK,
      final @Parameter(Launch.Rho.class) double rho,
      final GroupCommClient groupCommClient,
      final TaskEnvironment env) {

    this.startK = startK;
    this.endK = endK;
    this.dimK = dimK;
    this.rho = rho;
    this.env = env;

    final CommunicationGroupClient commGroup =
        groupCommClient.getCommunicationGroup(DESCRIPTOR.getCommGroupIdClass());

    this.modelReceiver = commGroup.getBroadcastReceiver(DESCRIPTOR.getBroadcastIdClass());
    this.resultSender = commGroup.getReduceSender(DESCRIPTOR.getReduceIdClass());

    LOG.log(Level.FINEST, "AlsTask {0} created: k = {1} range = {2}..{3}",
        new Object[] { taskId, dimK, startK, endK });
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    LOG.log(Level.FINEST, "AlsTask started");

    final DoubleMatrix[] ABC = { null, null, null };

    final DoubleMatrix tSlice = this.env.getT();

    LOG.log(Level.FINEST, "AlsTask start: Ta = {0}", tSlice);

    int iter = 0;
    for (;;) {

      // LOG.log(Level.FINEST, "AlsTask iteration {0}", iter);

      final DoubleMatrix[] receivedMatrix = this.modelReceiver.receive();
      if (receivedMatrix.length == 0) {
        break;
      }

      final int mode = (int) receivedMatrix[0].get(0);
      final int[] modeIdx = AlsMasterTask.ABC_ORDER[mode];

      ABC[modeIdx[1]] = receivedMatrix[1];

      if (receivedMatrix[2] != null) {
        ABC[modeIdx[2]] = receivedMatrix[2];
      }

      final DoubleMatrix B = ABC[modeIdx[1]];
      final DoubleMatrix C = ABC[modeIdx[2]];

      if (mode == 0) {
        ++iter;
      }

      // LOG.log(Level.FINE,
      //     "AlsTask iteration {0} mode: {1}", new Object[] { iter, "ABC".charAt(mode) });

      final DoubleMatrix CB = C.transpose().mmul(C).mul(B.transpose().mmul(B));
      final double r = this.rho * CB.mul(CB).sum() / this.dimK;
      LOG.log(Level.FINEST, "AlsTask iteration {0} :: r = {1}", new Object[] { iter, r });

      for (int i = 0; i < this.dimK; ++i) {
        CB.put(i, i, CB.get(i, i) + r);
      }

      final DoubleMatrix CBinv = TensorUtil.pinv(CB);

      final ArrayList<AbstractMap.SimpleEntry<Integer, DoubleMatrix>> rows = new ArrayList<>(tSlice.rows);

      for (int i = 0; i < tSlice.rows; ++i) {
        final DoubleMatrix kr = TensorUtil.multiKhatriRao(tSlice, i, C, B, r);
        LOG.log(Level.FINEST, "AlsTask t[{0}] = {1}", new Object[] { this.startK + i, tSlice.getRow(i) });
        LOG.log(Level.FINEST, "AlsTask B = {0}; C = {1}", new Object[] { B, C });
        LOG.log(Level.FINEST, "AlsTask kr[{0}] = {1}", new Object[] { this.startK + i, kr });
        rows.add(new AbstractMap.SimpleEntry<>(this.startK + i, kr.transpose().mmul(CBinv)));
      }

      LOG.log(Level.FINE,
          "AlsTask iteration {0} complete. Rows processed: {1}", new Object[] { iter, rows });

      this.resultSender.send(rows);
    }

    final DoubleMatrix[] receivedMatrix = this.modelReceiver.receive();

    this.env.clearT().setA(receivedMatrix[0]);

    LOG.log(Level.FINEST, "AlsTask complete after {0} iterations", iter);

    return null;
  }
}
