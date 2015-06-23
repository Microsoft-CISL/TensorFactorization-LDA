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
package com.microsoft.canberra.tf.task.m1;

import com.microsoft.canberra.tf.Launch;
import com.microsoft.canberra.tf.taskgroup.TaskGroupDescriptor;
import com.microsoft.canberra.tf.task.TaskEnvironment;
import com.microsoft.canberra.tf.task.Document;
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
public final class M1Task implements Task {

  private static final TaskGroupDescriptor DESCRIPTOR = M1TaskGroupDescriptor.INSTANCE;

  private static final Logger LOG = Logger.getLogger(M1Task.class.getName());

  private final int dimD;
  private final int dimK;

  private final TaskEnvironment env;

  private final Broadcast.Receiver<DoubleMatrix> modelReceiver;
  private final Reduce.Sender<DoubleMatrix[]> resultSender;

  @Inject
  public M1Task(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final @Parameter(Launch.DimD.class) int dimD,
      final @Parameter(Launch.DimK.class) int dimK,
      final GroupCommClient groupCommClient,
      final TaskEnvironment env) {

    final CommunicationGroupClient commGroup =
        groupCommClient.getCommunicationGroup(DESCRIPTOR.getCommGroupIdClass());

    this.dimD = dimD;
    this.dimK = dimK;
    this.env = env;

    this.modelReceiver = commGroup.getBroadcastReceiver(DESCRIPTOR.getBroadcastIdClass());
    this.resultSender = commGroup.getReduceSender(DESCRIPTOR.getReduceIdClass());

    LOG.log(Level.FINEST, "M1Task {0} created: d*k = {1} * {2}", new Object[]{ taskId, dimD, dimK });
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    final int examples = this.env.getExamples();
    final DoubleMatrix omega = this.env.getOmega();

    LOG.log(Level.FINEST, "M1Task started: {0} examples", examples);

    LOG.log(Level.FINEST, "M1Task omega[0] = {0}", omega.getRow(0));

    final DoubleMatrix m1 = DoubleMatrix.zeros(this.dimK);
    // final DoubleMatrix m1raw = DoubleMatrix.zeros(this.dimD);
    final DoubleMatrix wc = DoubleMatrix.zeros(examples, this.dimK);
    final DoubleMatrix counts = DoubleMatrix.zeros(examples);

    int i = 0;
    final DoubleMatrix wcRow = DoubleMatrix.zeros(this.dimK);
    final DoubleMatrix m1Row = DoubleMatrix.zeros(this.dimD);

    final DoubleMatrix sigma = this.modelReceiver.receive();
    sigma.addi(1e-6 * sigma.get(0));

    for (final Document doc : this.env.getDocuments()) {

      wcRow.fill(0);
      m1Row.fill(0);
      double totalCount = 0;
      for (final Document.Token token : doc) {
        m1Row.put(token.id, m1Row.get(token.id) + token.count);
        wcRow.addi(omega.getRow(token.id).mul(token.count));
        totalCount += token.count;
      }

      wcRow.divi(sigma);
      wc.putRow(i, wcRow);

      // LOG.log(Level.FINEST, "M1Task wc {0} count = {1} :: {2}",
      //     new Object[] { doc.getId(), totalCount, wcRow });

      m1.addi(wcRow.divi(totalCount));
      // m1raw.addi(m1Row.divi(totalCount));
      counts.put(i, totalCount);
      ++i;
    }

    this.resultSender.send(new DoubleMatrix[] { m1 }); // , m1raw });

    this.env.setWc(wc)
            .setCounts(counts)
            .clearDocuments();

    LOG.log(Level.FINEST, "M1Task complete");

    return null;
  }
}
