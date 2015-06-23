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
package com.microsoft.canberra.tf.task;

import org.apache.reef.annotations.audience.TaskSide;

import org.jblas.DoubleMatrix;

import javax.inject.Inject;
import java.util.List;

@TaskSide
public final class TaskEnvironment {

  private List<Document> documents = null;
  private int examples = -1;
  private DoubleMatrix omega = null;
  private DoubleMatrix sigma = null;
  private DoubleMatrix m1 = null;
  private DoubleMatrix m1raw = null;
  private DoubleMatrix wc = null;
  private DoubleMatrix counts = null;
  private DoubleMatrix t = null;
  private DoubleMatrix a = null;
  private DoubleMatrix lambda = null;

  @Inject
  public TaskEnvironment() {}

  public List<Document> getDocuments() {
    return this.documents;
  }

  public TaskEnvironment setDocuments(final List<Document> documents) {
    assert(this.documents == null);
    this.documents = documents;
    return this;
  }

  public TaskEnvironment clearDocuments() {
    assert(this.documents != null);
    this.documents = null;
    return this;
  }

  public int getExamples() {
    return this.examples;
  }

  public TaskEnvironment setExamples(final int examples) {
    assert(this.examples < 0);
    this.examples = examples;
    return this;
  }

  public DoubleMatrix getOmega() {
    return this.omega;
  }

  public TaskEnvironment setOmega(final DoubleMatrix omega) {
    assert(this.omega == null);
    this.omega = omega;
    return this;
  }

  public TaskEnvironment clearOmega() {
    assert(this.omega != null);
    this.omega = null;
    return this;
  }

  public DoubleMatrix getSigma() {
    return this.sigma;
  }

  public TaskEnvironment setSigma(final DoubleMatrix sigma) {
    assert(this.sigma == null);
    this.sigma = sigma;
    return this;
  }

  public TaskEnvironment clearSigma() {
    assert(this.sigma != null);
    this.sigma = null;
    return this;
  }

  public DoubleMatrix getM1() {
    return this.m1;
  }

  public TaskEnvironment setM1(final DoubleMatrix m1) {
    assert(this.m1 == null);
    this.m1 = m1;
    return this;
  }

  public TaskEnvironment clearM1() {
    assert(this.m1 != null);
    this.m1 = null;
    return this;
  }

  public DoubleMatrix getM1raw() {
    return this.m1raw;
  }

  public TaskEnvironment setM1raw(final DoubleMatrix m1raw) {
    assert(this.m1raw == null);
    this.m1raw = m1raw;
    return this;
  }

  public TaskEnvironment clearM1raw() {
    assert(this.m1raw != null);
    this.m1raw = null;
    return this;
  }

  public DoubleMatrix getWc() {
    return this.wc;
  }

  public TaskEnvironment setWc(final DoubleMatrix wc) {
    assert(this.wc == null);
    this.wc = wc;
    return this;
  }

  public TaskEnvironment clearWc() {
    assert(this.wc != null);
    return this;
  }

  public DoubleMatrix getCounts() {
    return this.counts;
  }

  public TaskEnvironment setCounts(final DoubleMatrix counts) {
    assert(this.counts == null);
    this.counts = counts;
    return this;
  }

  public TaskEnvironment clearCounts() {
    assert(this.counts != null);
    this.counts = null;
    return this;
  }

  public DoubleMatrix getT() {
    return this.t;
  }

  public TaskEnvironment setT(final DoubleMatrix t) {
    assert(this.t == null);
    this.t = t;
    return this;
  }

  public TaskEnvironment clearT() {
    assert(this.t != null);
    this.t = null;
    return this;
  }

  public DoubleMatrix getA() {
    return this.a;
  }

  public TaskEnvironment setA(final DoubleMatrix a) {
    assert(this.a == null);
    this.a = a;
    return this;
  }

  public TaskEnvironment clearA() {
    assert(this.a != null);
    this.a = null;
    return this;
  }

  public DoubleMatrix getLambda() {
    return this.lambda;
  }

  public TaskEnvironment setLambda(final DoubleMatrix lambda) {
    assert(this.lambda == null);
    this.lambda = lambda;
    return this;
  }

  public TaskEnvironment clearLambda() {
    assert(this.lambda != null);
    this.lambda = null;
    return this;
  }
}
