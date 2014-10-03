package de.kp.spark.intent.markov.hadoop
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Intent project
* (https://github.com/skrusche63/spark-intent).
* 
* Spark-Intent is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-Intent is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-Intent. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import java.io.{DataInput,DataOutput}
import org.apache.hadoop.io.Writable

import org.apache.mahout.math.DenseMatrix
import scala.collection.JavaConversions._

class DenseMatrixWritable extends Writable {

  private var m:DenseMatrix = null
  
  def this(v:DenseMatrix) {
    this()
   
    this.m = m
    
  }
  
  override def readFields(in:DataInput) {	
    
    val cols = in.readInt
    val rows = in.readInt
    
    val values = Array.fill[Double](rows,cols)(0.0) 
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
       
        values(i).update(j, in.readDouble)
      }
      
    }
    
    this.m = new DenseMatrix(values)
    
  }

  override def write(out:DataOutput) {

    val cols = m.columnSize
    val rows = m.rowSize
    
    /* dimensions */
    out.writeInt(cols)
    out.writeInt(rows)
    /* values */
    for (i <- 0 until rows) {
      
      val row = m.viewRow(i)
      for (element <- row.all) {
        out.writeDouble(element.get())      
      }
      
    }
    
  }

  def get():DenseMatrix = this.m
  
}