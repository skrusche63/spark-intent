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

import org.apache.mahout.math.DenseVector

class DenseVectorWritable extends Writable {

  private var v:DenseVector = null
  
  def this(v:DenseVector) {
    this()
   
    this.v = v
    
  }
  
  override def readFields(in:DataInput) {	
    
    val values = Array.fill[Double](in.readInt)(0.0) 
    for (i <- 0 until values.length) {
      values(i) = in.readDouble
    }

    this.v = new DenseVector(values)
    
  }

  override def write(out:DataOutput) {

    /* size */
    out.writeInt(v.size())
    /* elements */
    val iterator= v.iterator()
    while (iterator.hasNext()) {
      
      val element = iterator.next()
      out.writeDouble(element.get())
      
    }
    
  }

  def get():DenseVector = this.v
  
}