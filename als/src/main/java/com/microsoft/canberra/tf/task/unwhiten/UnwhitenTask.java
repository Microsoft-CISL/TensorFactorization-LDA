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
public final class UnwhitenTask implements Task {

  private static final TaskGroupDescriptor DESCRIPTOR = UnwhitenTaskGroupDescriptor.INSTANCE;

  private static final Logger LOG = Logger.getLogger(UnwhitenTask.class.getName());

  private final int dimD;
  private final int dimK;

  private final TaskEnvironment env;

  private final Broadcast.Receiver<DoubleMatrix[]> modelReceiver;
  private final Reduce.Sender<ArrayList<AbstractMap.SimpleEntry<Integer, DoubleMatrix>>> resultSender;

  @Inject
  public UnwhitenTask(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final @Parameter(Launch.DimD.class) int dimD,
      final @Parameter(Launch.DimK.class) int dimK,
      final GroupCommClient groupCommClient,
      final TaskEnvironment env) {

    this.dimD = dimD;
    this.dimK = dimK;
    this.env = env;

    final CommunicationGroupClient commGroup =
        groupCommClient.getCommunicationGroup(DESCRIPTOR.getCommGroupIdClass());

    this.modelReceiver = commGroup.getBroadcastReceiver(DESCRIPTOR.getBroadcastIdClass());
    this.resultSender = commGroup.getReduceSender(DESCRIPTOR.getReduceIdClass());

    LOG.log(Level.FINEST,
        "UnwhitenTask {0} created: d*k = {1} * {2}", new Object[] { taskId, dimD, dimK });
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    LOG.log(Level.FINEST, "UnwhitenTask started");

    LOG.log(Level.FINEST, "UnwhitenTask complete");

    return null;
  }
}
