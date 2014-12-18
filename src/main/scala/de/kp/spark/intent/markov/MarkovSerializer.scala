package de.kp.spark.intent.markov
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

class MarkovSerializer {

  /**
   * Build a serialized version from the variables, scala, states and 
   * transition matrix
   */
  def serialize(scale:Int,states:Array[String],matrix:TransitionMatrix):String = {
    
    val str_scale = scale.toString
    val str_states = states.mkString(",")
    
    val str_matrix = matrix.serialize
    
    String.format("""%s#%s#%s""",str_scale,str_states,str_matrix)
    
  }
  /**
   * Reconstruct scale, states and transition matrix from a serialized
   * representation
   */
  def deserialize(model:String):(Int,Array[String],TransitionMatrix) = {
    
    val Array(str_scale,str_states,str_matrix) = model.split("#")
    
    val scale = str_scale.toInt
    val states = str_states.split(",")

    val dim = states.length
	val matrix = new TransitionMatrix(dim,dim)
      
    matrix.setScale(scale)
    matrix.setStates(states,states)

    matrix.deserialize(str_matrix)

    (scale,states,matrix)
    
  }
  
}