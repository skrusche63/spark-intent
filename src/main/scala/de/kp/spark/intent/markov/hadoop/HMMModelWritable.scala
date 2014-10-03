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

import org.apache.mahout.math.{DenseMatrix,DenseVector}

import de.kp.scala.hmm.HmmModel

class HMMModelWritable extends Writable {

  private var AWritable = new DenseMatrixWritable()
  private var BWritable = new DenseMatrixWritable()
  
  private var PiWritable = new DenseVectorWritable()
  
  def this(model:HmmModel) {
    this()
    
    this.AWritable = new DenseMatrixWritable(model.getAMatrix)
    this.BWritable = new DenseMatrixWritable(model.getBMatrix)
    
    this.PiWritable = new DenseVectorWritable(model.getPiVector)
    
  }
  
  override def readFields(in:DataInput) {	
    
    this.AWritable.readFields(in)
    this.BWritable.readFields(in)
    
    this.PiWritable.readFields(in)
    
  }

  override def write(out:DataOutput) {
    
    /* Write A matrix */
    this.AWritable.write(out)
    /* Write B matrix */
    this.BWritable.write(out)
    /* Write Pi Vector */
    this.PiWritable.write(out)
    
  }

  def get():HmmModel = {
    
    val A = AWritable.get
    val B = BWritable.get
    
    val Pi = PiWritable.get
    
    new HmmModel(A,B,Pi)
    
  }
  
}