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

import com.microsoft.canberra.tf.task.MatrixSumReducer;
import com.microsoft.canberra.tf.taskgroup.TaskGroupDescriptor;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@DriverSide
public final class UnwhitenTaskGroupDescriptor extends TaskGroupDescriptor {

  @NamedParameter private static final class UnwhitenCommGroupId implements Name<String> {}
  @NamedParameter private static final class UnwhitenBroadcastId implements Name<String> {}
  @NamedParameter private static final class UnwhitenReduceId implements Name<String> {}

  public static final TaskGroupDescriptor INSTANCE = new UnwhitenTaskGroupDescriptor();

  private UnwhitenTaskGroupDescriptor() {
    super("TF-Unwhiten-", UnwhitenCommGroupId.class, UnwhitenBroadcastId.class, UnwhitenReduceId.class,
          MatrixSumReducer.class, UnwhitenMasterTask.class, UnwhitenTask.class);
  }
}
