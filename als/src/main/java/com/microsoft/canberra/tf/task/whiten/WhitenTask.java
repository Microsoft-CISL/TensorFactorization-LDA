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

import com.microsoft.canberra.tf.task.InputData;
import com.microsoft.canberra.tf.Launch;
import com.microsoft.canberra.tf.task.TaskEnvironment;
import com.microsoft.canberra.tf.taskgroup.TaskGroupDescriptor;
import com.microsoft.canberra.tf.task.Document;
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
import java.util.logging.Level;
import java.util.logging.Logger;

@TaskSide
public final class WhitenTask implements Task {

  private static final TaskGroupDescriptor DESCRIPTOR = WhitenTaskGroupDescriptor.INSTANCE;

  private static final Logger LOG = Logger.getLogger(WhitenTask.class.getName());

  private final int dimD;
  private final int dimKprime;

  private final TaskEnvironment env;
  private final Broadcast.Receiver<DoubleMatrix> modelReceiver;
  private final Reduce.Sender<DoubleMatrix[]> resultSender;

  @Inject
  public WhitenTask(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final @Parameter(Launch.DimD.class) int dimD,
      final @Parameter(Launch.DimKPrime.class) int dimKprime,
      final GroupCommClient groupCommClient,
      final InputData data,
      final TaskEnvironment env) {

    this.dimD = dimD;
    this.dimKprime = dimKprime;
    this.env = env;

    final CommunicationGroupClient commGroup =
        groupCommClient.getCommunicationGroup(DESCRIPTOR.getCommGroupIdClass());

    this.modelReceiver = commGroup.getBroadcastReceiver(DESCRIPTOR.getBroadcastIdClass());
    this.resultSender = commGroup.getReduceSender(DESCRIPTOR.getReduceIdClass());

    LOG.log(Level.FINEST,
        "WhitenTask {0} created: d*k_prime = {1} * {2}", new Object[] { taskId, dimD, dimKprime });

    env.setDocuments(data.getDocuments());
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    LOG.log(Level.FINEST, "WhitenTask started");

    int examples = 0;
    DoubleMatrix omega = TensorUtil.gaussian(dimD, dimKprime, WhitenMasterTask.SEED_OMEGA);

    final DoubleMatrix muX = DoubleMatrix.zeros(this.dimD);
    final DoubleMatrix Y = DoubleMatrix.zeros(this.dimD, this.dimKprime);
    final DoubleMatrix xOmega = DoubleMatrix.zeros(this.dimKprime);

    for (int i = 0; i < 2; ++i) {

      LOG.log(Level.FINEST, "WhitenTask iteration {0} start", i);

      muX.fill(0);
      Y.fill(0);
      examples = 0;
      for (final Document doc : this.env.getDocuments()) {

        ++examples;

        // LOG.log(Level.FINEST,
        //     "WhitenTask iteration {0} example {1} = {2}", new Object[] { i, examples, doc });

        xOmega.fill(0);
        double totalCount = 0;
        for (final Document.Token token : doc) {
          xOmega.addi(omega.getRow(token.id).mul(token.count));
          totalCount += token.count;
        }

        if (totalCount >= 2) {

          final double denom = 1.0 / (totalCount * (totalCount - 1.0));

          for (final Document.Token token : doc) {
            final DoubleMatrix row = Y.getRow(token.id);
            row.addi(xOmega.sub(omega.getRow(token.id)).mul(token.count * denom));
            Y.putRow(token.id, row);
            muX.put(token.id, muX.get(token.id) + token.count / totalCount);
          }
        }
      }

      LOG.log(Level.FINEST, "WhitenTask iteration {0} send muX, Y", i);

      this.resultSender.send(new DoubleMatrix[] { DoubleMatrix.scalar(examples), muX, Y });

      LOG.log(Level.FINEST, "WhitenTask iteration {0} receive omega", i);

      omega = this.modelReceiver.receive();
    }

    this.env.setOmega(omega).setExamples(examples);

    LOG.log(Level.FINEST, "WhitenTask complete: {0} examples", examples);

    return null;
  }
}
