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

import com.microsoft.canberra.tf.task.TaskEnvironment;
import com.microsoft.canberra.tf.taskgroup.TaskGroup;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import java.util.logging.Level;
import java.util.logging.Logger;

@DriverSide
public final class SharedContextTaskGroup implements TaskGroup {

  @NamedParameter public static final class StartK implements Name<Integer> {}
  @NamedParameter public static final class EndK implements Name<Integer> {}

  private static final Logger LOG = Logger.getLogger(SharedContextTaskGroup.class.getName());
  private static final Tang TANG = Tang.Factory.getTang();

  private final int numPartitions;
  private final String contextPrefix;
  private final String outputPath;
  private final int dimD;
  private final int dimK;
  private final int dimKprime;
  private final double alpha0;
  private final double rho;
  private final double tolerance;
  private final int maxIterations;

  private int taskNo = 0;
  private int tensorChunksAvailable;

  public SharedContextTaskGroup(
      final DriverEnvironment env, final String contextPrefix,
      final String outputPath, final int dimD, final int dimK, final int dimKprime,
      final double alpha0, final double rho, final double tolerance, final int maxIterations) {

    this.numPartitions = env.numPartitions;
    this.contextPrefix = contextPrefix;
    this.outputPath = outputPath;
    this.dimD = dimD;
    this.dimK = dimK;
    this.dimKprime = dimKprime;
    this.alpha0 = alpha0;
    this.rho = rho;
    this.tolerance = tolerance;
    this.maxIterations = maxIterations;
    this.tensorChunksAvailable = this.dimK;
  }

  @Override
  public void submit(final ActiveContext context, final boolean isMaster) {

    final String newContextId;
    final int startK;
    final int endK;

    if (isMaster) {

      newContextId = this.contextPrefix + "Master";
      startK = 0;
      endK = this.dimK;

    } else synchronized (this) {

      final int chunkSize = this.tensorChunksAvailable / (this.numPartitions - this.taskNo);
      this.tensorChunksAvailable -= chunkSize;
      startK = this.tensorChunksAvailable;
      endK = startK + chunkSize;
      newContextId = this.contextPrefix + this.taskNo;
      ++this.taskNo;
    }

    LOG.log(Level.FINER,
      "Submit TF Service: {0} of {1} context: {2} chunks {3}-{4}",
      new Object[] { newContextId, this.numPartitions, context.getId(), startK, endK });

    final Configuration contextConfig = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, newContextId)
            .build();

    final Configuration serviceConfig = TANG
        .newConfigurationBuilder(ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, TaskEnvironment.class)
            .build())
        .bindNamedParameter(Launch.Output.class, this.outputPath)
        .bindNamedParameter(Launch.DimD.class, "" + this.dimD)
        .bindNamedParameter(Launch.DimK.class, "" + this.dimK)
        .bindNamedParameter(Launch.DimKPrime.class, "" + this.dimKprime)
        .bindNamedParameter(StartK.class, "" + startK)
        .bindNamedParameter(EndK.class, "" + endK)
        .bindNamedParameter(Launch.Alpha0.class, "" + this.alpha0)
        .bindNamedParameter(Launch.Rho.class, "" + this.rho)
        .bindNamedParameter(Launch.Tolerance.class, "" + this.tolerance)
        .bindNamedParameter(Launch.MaxIterations.class, "" + this.maxIterations)
        .build();

    context.submitContextAndService(contextConfig, serviceConfig);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + ":" + this.contextPrefix + "*";
  }
}
