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
import com.microsoft.canberra.tf.task.MatrixSumReducer;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@DriverSide
public final class M3TaskGroupDescriptor extends TaskGroupDescriptor {

  @NamedParameter private static final class M3CommGroupId implements Name<String> {}
  @NamedParameter private static final class M3BroadcastId implements Name<String> {}
  @NamedParameter private static final class M3ReduceId implements Name<String> {}

  public static final TaskGroupDescriptor INSTANCE = new M3TaskGroupDescriptor();

  private M3TaskGroupDescriptor() {
    super("TF-M3-", M3CommGroupId.class, M3BroadcastId.class, M3ReduceId.class,
          MatrixSumReducer.class, M3MasterTask.class, M3Task.class);
  }
}
