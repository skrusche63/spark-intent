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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._
import scala.collection.mutable.HashMap

private case class Pair(ant:String,con:String)

class MarkovTrainer(scale:Int,states:Array[String]) extends Serializable {

  def build(rawset:RDD[Behavior]):TransitionMatrix = {
    /*
     * STEP #1: Determine the transitions (pairs of subsequent states)
     * from the time ordered states of every (site,user) and compute
     * the respective pair frequency
     */
    val pairs = rawset.flatMap(x => {
      
      val (site,user,states) = (x.site,x.user,x.states)
      states.zip(states.tail).map(v => Pair(v._1,v._2))
      
    })

    val pair_freq = pairs.groupBy(x => x).map(x => (x._1,x._2.size)).collect

    /* 
     * STEP #2: Setup transition matrix,add pair frequency
     */  	
    val dim = states.length
    
    val matrix = new TransitionMatrix(dim,dim)
    matrix.setScale(scale)
    
    matrix.setStates(states, states)    
    for ((pair,freq) <- pair_freq) {
      matrix.add(pair.ant, pair.con, freq)
    }
            
    /* Normalize the matrix content and transform support into probabilities */
	matrix.normalize()

    matrix
    
  }
  
}