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
package com.microsoft.canberra.tf.task.whiten;

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
import org.jblas.Eigen;
import org.jblas.MatrixFunctions;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

@TaskSide
public final class WhitenMasterTask implements Task {

  private static final TaskGroupDescriptor DESCRIPTOR = WhitenTaskGroupDescriptor.INSTANCE;

  private static final Logger LOG = Logger.getLogger(WhitenMasterTask.class.getName());

  static final long SEED_OMEGA = 1412218259;

  private final int dimD;
  private final int dimK;
  private final int dimKprime;
  private final double alpha0;
  private final TaskEnvironment env;

  private final Broadcast.Sender<DoubleMatrix> modelSender;
  private final Reduce.Receiver<DoubleMatrix[]> resultReceiver;

  @Inject
  public WhitenMasterTask(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final @Parameter(Launch.Alpha0.class) double alpha0,
      final @Parameter(Launch.DimD.class) int dimD,
      final @Parameter(Launch.DimK.class) int dimK,
      final @Parameter(Launch.DimKPrime.class) int dimKprime,
      final GroupCommClient groupCommClient,
      final TaskEnvironment env) {

    this.dimD = dimD;
    this.dimK = dimK;
    this.dimKprime = dimKprime;
    this.alpha0 = alpha0;
    this.env = env;

    final CommunicationGroupClient commGroup =
        groupCommClient.getCommunicationGroup(DESCRIPTOR.getCommGroupIdClass());

    this.modelSender = commGroup.getBroadcastSender(DESCRIPTOR.getBroadcastIdClass());
    this.resultReceiver = commGroup.getReduceReceiver(DESCRIPTOR.getReduceIdClass());

    LOG.log(Level.FINEST,
        "WhitenMasterTask {0} created: d*k_prime = {1} * {2}",
        new Object[] { taskId, dimD, dimKprime });
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    LOG.log(Level.FINEST, "WhitenMasterTask started");

    int examples = 0;
    DoubleMatrix omega = TensorUtil.gaussian(this.dimD, this.dimKprime, SEED_OMEGA);
    DoubleMatrix sigma = null;

    LOG.log(Level.FINEST, "WhitenMasterTask init: omega = {0}", omega);

    for (int i = 0; i < 2; ++i) {

      LOG.log(Level.FINEST, "WhitenMasterTask iteration {0} start", i);

      final DoubleMatrix[] reduced = this.resultReceiver.reduce();

      examples = (int) reduced[0].get(0);
      final DoubleMatrix muX = reduced[1]; // 1*d
      final DoubleMatrix Y = reduced[2];   // d*k

      LOG.log(Level.FINEST,
          "WhitenMasterTask iteration {0}: {1} examples", new Object[]{ i, examples });

      // LOG.log(Level.FINEST, "WhitenMasterTask iteration {0}: muX = {1}", new Object[] { i, muX });
      // LOG.log(Level.FINEST, "WhitenMasterTask iteration {0}: Y = {1}", new Object[] { i, Y });

      final double scaleFactor = (1.0 + this.alpha0) / examples;

      Y.muli(scaleFactor);

      if (this.alpha0 > 0) {
        muX.divi(examples);
        final DoubleMatrix muXOmega = muX.transpose().mmul(omega);
        Y.subi(muX.mmul(muXOmega).muli(this.alpha0));
      }

      if (i == 0) {
        omega = TensorUtil.orthogonalize(Y);
      } else {

        final DoubleMatrix[] eigen = Eigen.symmetricEigenvectors(Y.transpose().mmul(Y));
        sigma = MatrixFunctions.sqrti(MatrixFunctions.sqrti(eigen[1].diag().maxi(0)));
        omega = Y.mmuli(eigen[0]).diviRowVector(sigma.add(1.0e-12));

        LOG.log(Level.FINEST, "WhitenMasterTask omega_pre = {0}", omega);
        LOG.log(Level.FINEST, "WhitenMasterTask sigma_pre = {0}", sigma);

        final int[] sortingIdx = Arrays.copyOfRange(sigma.neg().sortingPermutation(), 0, this.dimK);
        sigma = sigma.get(sortingIdx);
        omega = omega.getColumns(sortingIdx);

        for (int j = 0; j < omega.columns; ++j) {
          if (omega.get(0, j) < 0) {
            omega.putColumn(j, omega.getColumn(j).negi());
          }
        }
      }

      LOG.log(Level.FINEST, "WhitenMasterTask iteration {0} send omega", i);

      this.modelSender.send(omega);

      LOG.log(Level.FINEST, "WhitenMasterTask omega = {0}", omega);
      LOG.log(Level.FINEST, "WhitenMasterTask sigma = {0}", sigma);
    }

    assert(sigma != null);

    this.env.setOmega(omega)
            .setSigma(sigma)
            .setExamples(examples);

    LOG.log(Level.FINEST, "WhitenMasterTask complete: {0} examples", examples);

    return null;
  }
}
