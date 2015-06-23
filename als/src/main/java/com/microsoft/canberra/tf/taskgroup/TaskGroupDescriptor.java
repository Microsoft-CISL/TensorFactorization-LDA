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
package com.microsoft.canberra.tf.taskgroup;

import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.task.Task;
import org.apache.reef.tang.annotations.Name;

public class TaskGroupDescriptor {

  private final String taskPrefix;
  private final String masterTaskId;

  private final Class<? extends Name<String>> commGroupIdClass;
  private final Class<? extends Name<String>> broadcastIdClass;
  private final Class<? extends Name<String>> reduceIdClass;

  private final Class<? extends Reduce.ReduceFunction<?>> reducerClass;

  private final Class<? extends Task> masterTaskClass;
  private final Class<? extends Task> workerTaskClass;

  protected TaskGroupDescriptor(
      final String taskPrefix,
      final Class<? extends Name<String>> commGroupIdClass,
      final Class<? extends Name<String>> broadcastIdClass,
      final Class<? extends Name<String>> reduceIdClass,
      final Class<? extends Reduce.ReduceFunction<?>> reducerClass,
      final Class<? extends Task> masterTaskClass,
      final Class<? extends Task> workerTaskClass) {

    this.taskPrefix = taskPrefix;
    this.masterTaskId = taskPrefix + "Master";
    this.commGroupIdClass = commGroupIdClass;
    this.broadcastIdClass = broadcastIdClass;
    this.reduceIdClass = reduceIdClass;
    this.reducerClass = reducerClass;
    this.masterTaskClass = masterTaskClass;
    this.workerTaskClass = workerTaskClass;
  }

  public String getTaskPrefix() {
    return taskPrefix;
  }

  public String getMasterTaskId() {
    return masterTaskId;
  }

  public Class<? extends Name<String>> getCommGroupIdClass() {
    return commGroupIdClass;
  }

  public Class<? extends Name<String>> getBroadcastIdClass() {
    return broadcastIdClass;
  }

  public Class<? extends Name<String>> getReduceIdClass() {
    return reduceIdClass;
  }

  public Class<? extends Reduce.ReduceFunction<?>> getReducerClass() {
    return reducerClass;
  }

  public Class<? extends Task> getMasterTaskClass() {
    return masterTaskClass;
  }

  public Class<? extends Task> getWorkerTaskClass() {
    return workerTaskClass;
  }
}
