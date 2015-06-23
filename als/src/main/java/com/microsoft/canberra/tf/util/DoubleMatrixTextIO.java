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
package com.microsoft.canberra.tf.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.jblas.DoubleMatrix;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility methods to read and write jBLAS DoubleMatrix to/from HDFS.
 * This object is injected into the TF Driver.
 */
public final class DoubleMatrixTextIO {

  private static final Logger LOG = Logger.getLogger(DoubleMatrixTextIO.class.getName());

  private final FileSystem fileSystem;

  @Inject
  public DoubleMatrixTextIO() throws IOException {
    final YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    yarnConf.set("fs.file.impl", LocalFileSystem.class.getName());
    this.fileSystem = FileSystem.newInstance(yarnConf);
  }

  public BufferedReader hdfsReader(final String path) throws IOException {
    return new BufferedReader(new InputStreamReader(this.fileSystem.open(new Path(path))));
  }

  public BufferedWriter hdfsWriter(final String path) throws IOException {
    return new BufferedWriter(new OutputStreamWriter(this.fileSystem.create(new Path(path))));
  }

  /**
   * Read the entire text file from HDFS into jBLAS DoubleMatrix object.
   * Input must always be a single file (not a wildcard or a directory).
   * Rows are separated by newlines, columns are separated by tabs.
   * First column is an integer row number (starting from 0);
   * second column can be either blank or contain lambda values;
   * remaining columns are floating point numbers for the matrix.
   * First and second columns (i.e. row number and lambda) are ignored.
   * @param pathA Path to the text file on HDFS that contains a matrix.
   * @return jBLAS matrix.
   */
  public DoubleMatrix readMatrix(final String pathA) {

    LOG.log(Level.FINER, "Read matrix from: {0}", pathA);

    DoubleMatrix matrix = null;

    try (final BufferedReader reader = this.hdfsReader(pathA)) {

      for (String ln = reader.readLine(); ln != null; ln = reader.readLine()) {

        final String[] tabFields = ln.split("\t");

        final int i = Integer.parseInt(tabFields[0]);

        final List<Double> rowList = new ArrayList<>();
        for (final String valStr : tabFields[2].split(",")) {
          try {
            rowList.add(Double.valueOf(valStr));
          } catch (final NumberFormatException ex) {
            // ignore trailing comma etc.
            LOG.log(Level.FINEST, "Cannot parse number: {0}", valStr);
          }
        }

        final DoubleMatrix row = new DoubleMatrix(rowList);
        matrix = matrix == null ? row : DoubleMatrix.concatHorizontally(matrix, row);
      }

      return matrix == null ? DoubleMatrix.EMPTY : matrix.transpose();

    } catch (final IOException ex) {
      LOG.log(Level.SEVERE, "Cannot decode matrix", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Save the jBLAS DoubleMatrix object into a text file on HDFS.
   * Output path must always be a single file on HDFS that does not exists yet.
   * Rows are separated by newlines, columns are separated by tabs.
   * First column is an integer row number (starting from 0);
   * remaining columns are floating point numbers of the matrix.
   * @param matrix jBLAS matrix to write to the file system.
   * @param outputPath HDFS path to the file to write the matrix to.
   */
  public void writeMatrix(final DoubleMatrix matrix, final String outputPath) {

    LOG.log(Level.FINER, "Write matrix to: {0}", outputPath);

    try (final BufferedWriter writer = this.hdfsWriter(outputPath)) {

      for (int i = 0; i < matrix.rows; ++i) {
        writer.write(String.format("%d\t%s\n",
            i, matrix.getRow(i).toString("%f", "", "", "\t", "\n")));
      }

    } catch (final IOException ex) {
      LOG.log(Level.SEVERE, "Cannot write matrix to: " + outputPath, ex);
      throw new RuntimeException(ex);
    }
  }
}
