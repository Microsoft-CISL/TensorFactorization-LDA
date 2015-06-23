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

@TaskSide
public final class M3MasterTask implements Task {

  private static final TaskGroupDescriptor DESCRIPTOR = M3TaskGroupDescriptor.INSTANCE;

  private static final Logger LOG = Logger.getLogger(M3MasterTask.class.getName());

  private final TaskEnvironment env;

  private final Broadcast.Sender<DoubleMatrix> modelSender;
  private final Reduce.Receiver<DoubleMatrix[]> resultReceiver;

  @Inject
  public M3MasterTask(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final GroupCommClient groupCommClient,
      final TaskEnvironment env) {

    this.env = env;

    final CommunicationGroupClient commGroup =
        groupCommClient.getCommunicationGroup(DESCRIPTOR.getCommGroupIdClass());

    this.modelSender = commGroup.getBroadcastSender(DESCRIPTOR.getBroadcastIdClass());
    this.resultReceiver = commGroup.getReduceReceiver(DESCRIPTOR.getReduceIdClass());

    LOG.log(Level.FINEST, "M3MasterTask {0} created", taskId);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    LOG.log(Level.FINEST, "M3MasterTask started");

    this.modelSender.send(this.env.getM1());

    LOG.log(Level.FINEST, "Sent M1; waiting for M3");

    final DoubleMatrix received[] = this.resultReceiver.reduce();
    final double validExamples = received[0].get(0);
    final DoubleMatrix m3 = received[1];

    LOG.log(Level.FINEST, "M3MasterTask: got data for {0} valid examples; M3 = {1}",
        new Object[] { validExamples, m3 });

    this.modelSender.send(m3.divi(validExamples));

    LOG.log(Level.FINEST, "M3MasterTask complete");

    return null;
  }
}
