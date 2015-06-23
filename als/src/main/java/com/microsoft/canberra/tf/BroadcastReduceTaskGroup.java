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
package com.microsoft.canberra.tf;

import com.microsoft.canberra.tf.taskgroup.TaskGroup;
import com.microsoft.canberra.tf.taskgroup.TaskGroupDescriptor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ReduceOperatorSpec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.task.Task;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@DriverSide
public final class BroadcastReduceTaskGroup implements TaskGroup {

  private static final Logger LOG = Logger.getLogger(BroadcastReduceTaskGroup.class.getName());
  private static final Tang TANG = Tang.Factory.getTang();

  private ActiveContext masterContext = null;
  private final List<ActiveContext> workerContexts;
  private final AtomicInteger numContextsLeft;

  private final DriverEnvironment env;
  private final TaskGroupDescriptor tg;
  private final CommunicationGroupDriver commGroup;

  public BroadcastReduceTaskGroup(final DriverEnvironment env, final TaskGroupDescriptor tg) {

    this.env = env;
    this.tg = tg;

    final int numTasks = this.env.numPartitions + 1;

    this.commGroup = this.env.groupCommDriver.newCommunicationGroup(tg.getCommGroupIdClass(), numTasks);

    this.commGroup
        .addBroadcast(tg.getBroadcastIdClass(),
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(tg.getMasterTaskId())
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addReduce(tg.getReduceIdClass(),
            ReduceOperatorSpec.newBuilder()
                .setReceiverId(tg.getMasterTaskId())
                .setDataCodecClass(SerializableCodec.class)
                .setReduceFunctionClass(tg.getReducerClass())
                .build())
        .finalise();

    this.workerContexts = Collections.synchronizedList(new ArrayList<ActiveContext>(numTasks));
    this.numContextsLeft = new AtomicInteger(numTasks);
  }

  @Override
  public void submit(final ActiveContext context, final boolean isMaster) {

    if (isMaster) {
      assert(this.masterContext == null);
      this.masterContext = context;
    } else {
      this.workerContexts.add(context);
    }

    if (this.numContextsLeft.decrementAndGet() == 0) {

      LOG.log(Level.INFO, "Submit tasks for Task Group {0}", this);

      this.submit(this.masterContext, this.tg.getMasterTaskId(), this.tg.getMasterTaskClass());

      for (final ActiveContext workerContext : this.workerContexts) {
        final Class<? extends Task> clazz = this.tg.getWorkerTaskClass();
        this.submit(workerContext, this.tg.getTaskPrefix() + workerContext.getId(), clazz);
      }
    }
  }

  private void submit(final ActiveContext context,
                      final String taskId, final Class<? extends Task> taskClass) {

    LOG.log(Level.FINER, "Submit {0} id {1} to context: {2}",
        new Object[] { taskClass.getSimpleName(), taskId, context.getId() });

    final Configuration taskConfig = TANG
        .newConfigurationBuilder(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, taskClass)
            .build())
        .build();

    this.commGroup.addTask(taskConfig);
    context.submitTask(this.env.groupCommDriver.getTaskConfiguration(taskConfig));
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + ":" + this.tg.getTaskPrefix() + "*";
  }
}
