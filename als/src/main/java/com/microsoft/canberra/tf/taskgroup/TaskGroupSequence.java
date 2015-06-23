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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@DriverSide
public final class TaskGroupSequence {

  private static final Logger LOG = Logger.getLogger(TaskGroupSequence.class.getName());

  private final TaskGroup[] taskGroups;
  private final Map<String, Integer> evalTaskGroup = new HashMap<>();

  private int currentTaskGroupIdx = -1;
  private String masterEvalId = null;

  public TaskGroupSequence(final TaskGroup... taskGroups) {
    this.taskGroups = taskGroups;
  }

  /**
   * Return true if given context belongs to the master evaluator.
   * Also sets the masterEvalId to the ID of the master evaluator if current
   * context belongs to the master and the variable has not been set before.
   *
   * @param context current active context.
   * @return true if this is a master context, and false if it's a worker.
   */
  private boolean checkMaster(final ActiveContext context, final boolean isMaster) {

    final String evalId = context.getEvaluatorId().intern();
    if (this.masterEvalId == null && isMaster) {
      this.masterEvalId = evalId;
      return true;
    }

    return evalId.equals(this.masterEvalId);
  }

  /**
   * Get next TaskGroup to submit to the given context,
   * or null if we are done with this context.
   *
   * @param context Current active context.
   * @return next TaskGroup for submission, or null if done.
   */
  private TaskGroup nextTaskGroup(final ActiveContext context) {

    final String evalId = context.getEvaluatorId();

    Integer i = this.evalTaskGroup.get(evalId);
    if (i == null) {
      this.evalTaskGroup.put(evalId, 0);
      i = 0;
    }

    if (i >= this.taskGroups.length) {
      return null;
    }

    this.evalTaskGroup.put(evalId, i + 1);
    return this.taskGroups[i];
  }

  /**
   * Submit task from the next task group in the sequence. Close context if done.
   *
   * @param context Current active context.
   * @param isMaster Hint from the driver: true if current context is master.
   */
  public void submitNext(final ActiveContext context, boolean isMaster) {

    final TaskGroup taskGroup;
    synchronized (this) {
      isMaster = checkMaster(context, isMaster);
      taskGroup = this.nextTaskGroup(context);
    }

    LOG.log(Level.FINEST, "Submit to context: {0} evaluator: {1} master: {2} tg: {3}",
        new Object[] { context.getId(), context.getEvaluatorId(), isMaster, taskGroup });

    if (taskGroup != null) {
      taskGroup.submit(context, isMaster);
    } else {
      context.close();
    }
  }
}
