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

import com.microsoft.canberra.tf.util.SparseVectorInputFormat;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.api.DataLoadingRequestBuilder;
import org.apache.reef.io.network.group.impl.driver.GroupCommService;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.util.EnvironmentUtils;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@ClientSide
public class Launch {

  private static final int NUM_EVALUATORS = 4;

  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

  @NamedParameter(short_name = "input", doc = "Path to the input data")
  public static final class Input implements Name<String> {
  }

  @NamedParameter(short_name = "output", doc = "Output path")
  public static final class Output implements Name<String> {
  }

  @NamedParameter(short_name = "partitions", doc = "Number of partitions")
  public static final class NumPartitions implements Name<Integer> {
  }

  @NamedParameter(short_name = "mem_master", default_value = "1024",
      doc = "Amount of memory required for the master, M")
  public static final class MemMaster implements Name<Integer> {
  }

  @NamedParameter(short_name = "mem_worker", default_value = "1024",
      doc = "Amount of memory required for the worker, M")
  public static final class MemWorker implements Name<Integer> {
  }

  @NamedParameter(short_name = "d", doc = "Vocabulary size")
  public static final class DimD implements Name<Integer> {
  }

  @NamedParameter(short_name = "k", doc = "Final number of topics")
  public static final class DimK implements Name<Integer> {
  }

  @NamedParameter(short_name = "kprime", doc = "Number of topics for whitening")
  public static final class DimKPrime implements Name<Integer> {
  }

  @NamedParameter(short_name = "alpha0", doc = "Smoothing factor")
  public static final class Alpha0 implements Name<Double> {
  }

  @NamedParameter(short_name = "rho", default_value = "0",
      doc = "Proximal term strength")
  public static final class Rho implements Name<Double> {
  }

  @NamedParameter(short_name = "tolerance", default_value = "1e-4",
      doc = "Convergence threshold")
  public static final class Tolerance implements Name<Double> {
  }

  @NamedParameter(short_name = "max_iterations", default_value = "0",
      doc = "Max. number of iterations")
  public static final class MaxIterations implements Name<Integer> {
  }

  @NamedParameter(short_name = "local", default_value = "true",
                  doc = "Whether or not to run on the local runtime")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * @return (immutable) TANG Configuration object.
   * @throws BindException if configuration injector fails.
   */
  private static Configuration getRuntimeConfiguration(final boolean isLocal) throws BindException {
    if (isLocal) {
      LOG.log(Level.INFO, "Running on local runtime with {0} evaluators", NUM_EVALUATORS);
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, NUM_EVALUATORS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running on YARN");
      return YarnClientConfiguration.CONF.build();
    }
  }

  public static void main(final String[] args) {

    try {

      final Configuration commandLineConfig = new CommandLine()
          .registerShortNameOfClass(Input.class)
          .registerShortNameOfClass(Output.class)
          .registerShortNameOfClass(NumPartitions.class)
          .registerShortNameOfClass(MemMaster.class)
          .registerShortNameOfClass(MemWorker.class)
          .registerShortNameOfClass(DimD.class)
          .registerShortNameOfClass(DimK.class)
          .registerShortNameOfClass(DimKPrime.class)
          .registerShortNameOfClass(Alpha0.class)
          .registerShortNameOfClass(Rho.class)
          .registerShortNameOfClass(Tolerance.class)
          .registerShortNameOfClass(MaxIterations.class)
          .registerShortNameOfClass(Local.class)
          .processCommandLine(args)
          .getBuilder().build();

      final Injector injector = Tang.Factory.getTang().newInjector(commandLineConfig);
      final boolean isLocal = injector.getNamedInstance(Local.class);
      final String inputPath = injector.getNamedInstance(Input.class);
      final String outputPath = injector.getNamedInstance(Output.class);
      final int numPartitions = injector.getNamedInstance(NumPartitions.class);
      final int memMaster = injector.getNamedInstance(MemMaster.class);
      final int memWorker = injector.getNamedInstance(MemWorker.class);
      final int dimD = injector.getNamedInstance(DimD.class);
      final int dimK = injector.getNamedInstance(DimK.class);
      final int dimKprime = injector.getNamedInstance(DimKPrime.class);
      final double alpha0 = injector.getNamedInstance(Alpha0.class);
      final double rho = injector.getNamedInstance(Rho.class);
      final double tolerance = injector.getNamedInstance(Tolerance.class);
      final int maxIterations = injector.getNamedInstance(MaxIterations.class);

      final Configuration loaderConfig = new DataLoadingRequestBuilder()
          .setMemoryMB(memWorker)
          .setInputPath(inputPath)
          .renewFailedEvaluators(false)
          .setInputFormatClass(SparseVectorInputFormat.class)
          .setNumberOfDesiredSplits(numPartitions)
          .loadIntoMemory(false)
          .setDriverConfigurationModule(DriverConfiguration.CONF
              .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(Driver.class))
              .set(DriverConfiguration.DRIVER_IDENTIFIER, "TF")
              .set(DriverConfiguration.ON_CONTEXT_ACTIVE, Driver.ContextActiveHandler.class)
              .set(DriverConfiguration.ON_TASK_COMPLETED, Driver.TaskCompletedHandler.class))
          .setComputeRequest(EvaluatorRequest.newBuilder()
              .setNumber(1)
              .setMemory(memMaster)
              .build())
          .build();

      final Configuration config = Tang.Factory.getTang()
          .newConfigurationBuilder(loaderConfig, GroupCommService.getConfiguration())
          .bindNamedParameter(Output.class, outputPath)
          .bindNamedParameter(DimD.class, "" + dimD)
          .bindNamedParameter(DimK.class, "" + dimK)
          .bindNamedParameter(DimKPrime.class, "" + dimKprime)
          .bindNamedParameter(Alpha0.class, "" + alpha0)
          .bindNamedParameter(Rho.class, "" + rho)
          .bindNamedParameter(Tolerance.class, "" + tolerance)
          .bindNamedParameter(MaxIterations.class, "" + maxIterations)
          .build();

      LOG.log(Level.FINEST, "Driver configuration:\n--\n{0}\n--",
          new AvroConfigurationSerializer().toString(config));

      final LauncherStatus status = DriverLauncher.getLauncher(
          getRuntimeConfiguration(isLocal)).run(config);

      LOG.log(Level.INFO, "Job completed: {0}", status);

    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }
}
