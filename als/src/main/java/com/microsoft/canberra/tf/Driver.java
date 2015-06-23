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

import com.microsoft.canberra.tf.task.als.AlsTaskGroupDescriptor;
import com.microsoft.canberra.tf.task.m1.M1TaskGroupDescriptor;
import com.microsoft.canberra.tf.task.m3.M3TaskGroupDescriptor;
import com.microsoft.canberra.tf.task.unwhiten.UnwhitenTaskGroupDescriptor;
import com.microsoft.canberra.tf.task.whiten.WhitenTaskGroupDescriptor;
import com.microsoft.canberra.tf.taskgroup.TaskGroupSequence;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.network.group.api.driver.GroupCommDriver;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
@DriverSide
public final class Driver {

  private static final Logger LOG = Logger.getLogger(Driver.class.getName());

  private final DataLoadingService dataLoadingService;
  private final TaskGroupSequence taskGroups;

  @Inject
  public Driver(final DataLoadingService dataLoadingService,
                final GroupCommDriver groupCommDriver,
                final @Parameter(Launch.Output.class) String outputPath,
                final @Parameter(Launch.DimD.class) int dimD,
                final @Parameter(Launch.DimK.class) int dimK,
                final @Parameter(Launch.DimKPrime.class) int dimKprime,
                final @Parameter(Launch.Alpha0.class) double alpha0,
                final @Parameter(Launch.Rho.class) double rho,
                final @Parameter(Launch.Tolerance.class) Double tolerance,
                final @Parameter(Launch.MaxIterations.class) Integer maxIterations) {

    this.dataLoadingService = dataLoadingService;

    final DriverEnvironment env = new DriverEnvironment(
        groupCommDriver, dataLoadingService.getNumberOfPartitions());

    this.taskGroups = new TaskGroupSequence(
        new SharedContextTaskGroup(env, "TF-Environment-",
            outputPath, dimD, dimK, dimKprime, alpha0, rho, tolerance, maxIterations),
        new GroupCommTaskGroup(env),
        new BroadcastReduceTaskGroup(env, WhitenTaskGroupDescriptor.INSTANCE),
        new BroadcastReduceTaskGroup(env, M1TaskGroupDescriptor.INSTANCE),
        new BroadcastReduceTaskGroup(env, M3TaskGroupDescriptor.INSTANCE),
        new BroadcastReduceTaskGroup(env, AlsTaskGroupDescriptor.INSTANCE),
        new BroadcastReduceTaskGroup(env, UnwhitenTaskGroupDescriptor.INSTANCE));
  }

  public class ContextActiveHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      LOG.log(Level.INFO, "Active context: {0}", context.getId());
      taskGroups.submitNext(context, dataLoadingService.isComputeContext(context));
    }
  }

  public class TaskCompletedHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {
      final ActiveContext context = task.getActiveContext();
      LOG.log(Level.INFO, "Task completed: {0} context: {1}",
          new Object[]{ task.getId(), context.getId() });
      taskGroups.submitNext(context, dataLoadingService.isComputeContext(context));
    }
  }
}
