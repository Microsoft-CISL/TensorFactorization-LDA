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
package com.microsoft.canberra.tf.task.unwhiten;

import com.microsoft.canberra.tf.Launch;
import com.microsoft.canberra.tf.task.TaskEnvironment;
import com.microsoft.canberra.tf.taskgroup.TaskGroupDescriptor;

import com.microsoft.canberra.tf.util.DoubleMatrixTextIO;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.task.Task;
import org.apache.reef.tang.annotations.Parameter;

import org.jblas.DoubleMatrix;
import org.jblas.Solve;

import javax.inject.Inject;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

@TaskSide
public final class UnwhitenMasterTask implements Task {

  private static final TaskGroupDescriptor DESCRIPTOR = UnwhitenTaskGroupDescriptor.INSTANCE;

  private static final Logger LOG = Logger.getLogger(UnwhitenMasterTask.class.getName());

  private static final int MAX_UNWHITENING_ITERATIONS = 20;

  private static final double RHO = 1.0;

  private final int dimK;
  private final double alpha0;
  private final String outputPath;

  private final DoubleMatrixTextIO hdfsIO;
  private final TaskEnvironment env;

  private final Broadcast.Sender<DoubleMatrix[]> modelSender;
  private final Reduce.Receiver<ArrayList<AbstractMap.SimpleEntry<Integer, DoubleMatrix>>> resultReceiver;

  @Inject
  public UnwhitenMasterTask(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final @Parameter(Launch.DimK.class) int dimK,
      final @Parameter(Launch.Alpha0.class) double alpha0,
      final @Parameter(Launch.Output.class) String outputPath,
      final GroupCommClient groupCommClient,
      final DoubleMatrixTextIO hdfsIO,
      final TaskEnvironment env) {

    this.dimK = dimK;
    this.alpha0 = alpha0;
    this.outputPath = outputPath;
    this.hdfsIO = hdfsIO;
    this.env = env;

    final CommunicationGroupClient commGroup =
        groupCommClient.getCommunicationGroup(DESCRIPTOR.getCommGroupIdClass());

    this.modelSender = commGroup.getBroadcastSender(DESCRIPTOR.getBroadcastIdClass());
    this.resultReceiver = commGroup.getReduceReceiver(DESCRIPTOR.getReduceIdClass());

    LOG.log(Level.FINEST, "UnwhitenMasterTask {0} created", taskId);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    LOG.log(Level.FINEST, "UnwhitenMasterTask started");

    final DoubleMatrix A = this.env.getA();     // k*k
    final DoubleMatrix W = this.env.getOmega(); // d*k
    final DoubleMatrix lambda = this.env.getLambda();

    LOG.log(Level.FINEST, "UnwhitenMasterTask: W = {0}", W);

    final DoubleMatrix alphaHat = DoubleMatrix.ones(this.dimK).divi(lambda).divi(lambda);
    final DoubleMatrix prior = alphaHat.div(alphaHat.sum());
    final DoubleMatrix alpha = prior.mul(this.alpha0);

    final DoubleMatrix F =
        W.mmul(Solve.solvePositive(W.transpose().mmul(W), A.mmul(DoubleMatrix.diag(lambda))));

    LOG.log(Level.FINEST, "UnwhitenMasterTask F = {0}", F);

    final DoubleMatrix z = DoubleMatrix.zeros(W.rows, A.columns);

    for (int i = 0; i < A.columns; ++i) {
      final DoubleMatrix col = F.getColumn(i);
      final DoubleMatrix v1 = unwhitenTopic(col);
      final DoubleMatrix v2 = unwhitenTopic(col.neg());
      z.putColumn(i, v1.sub(col).norm2() < v2.add(col).norm2() ? v1 : v2);
    }

    LOG.log(Level.FINEST, "UnwhitenMasterTask complete: z = {0}", z);

    this.hdfsIO.writeMatrix(alpha, this.outputPath + ".alpha");
    this.hdfsIO.writeMatrix(z, this.outputPath + ".beta");

    return null;
  }

  private static DoubleMatrix unwhitenTopic(final DoubleMatrix v) {

    DoubleMatrix z = DoubleMatrix.zeros(v.length);
    final DoubleMatrix u = DoubleMatrix.zeros(v.length);

    for (int i = 0; i < MAX_UNWHITENING_ITERATIONS; ++i) {
      final DoubleMatrix mu = v.add(z.sub(u).mul(RHO)).div(RHO + 1);
      z = simplexProjection(mu.add(u));
      u.addi(mu).subi(z);
    }

    return z;
  }

  public static DoubleMatrix simplexProjection(final DoubleMatrix v) {

    // LOG.log(Level.FINEST, "UnwhitenMasterTask simplexProjection: v = {0}", v);

    final DoubleMatrix mu = v.neg().sorti().negi(); // sort in descending order
    final DoubleMatrix theta = mu.cumulativeSum()
        .subi(1).divi(DoubleMatrix.linspace(1, mu.length, mu.length));

    int j = mu.length - 1;
    for (; j > 0 && mu.get(j) <= theta.get(j); --j) {}

    final DoubleMatrix res = v.sub(theta.get(j)).maxi(0);

    // LOG.log(Level.FINEST, "UnwhitenMasterTask simplexProjection: sum(res) = {0}", res.sum());

    return res;
  }
}
