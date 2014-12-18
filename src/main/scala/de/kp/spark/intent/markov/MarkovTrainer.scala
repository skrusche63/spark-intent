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

  def build(dataset:RDD[Behavior]):TransitionMatrix = {
   
    def seqOp(support:HashMap[Pair,Int],seq:Behavior):HashMap[Pair,Int] = {
          
      val (site,user,states) = (seq.site,seq.user,seq.states)
      /*
       *  The pair support aggregates over all sites and users provided,
       *  i.e. this is no personalized transition probability matrix
       */  
      for (i <- 1 until states.size) {
        
        val pair = new Pair(states(i-1),states(i))

        support.get(pair) match {          
          case None => support += pair -> 1
          case Some(count) => support += pair -> (count + 1)
        }

      }
      
      support
      
    }
    
    /* Note that supp1 is always NULL */
    def combOp(supp1:HashMap[Pair,Int],supp2:HashMap[Pair,Int]):HashMap[Pair,Int] = supp2      

    /* Build pair support */
    val pairsupp = dataset.coalesce(1, false).aggregate(HashMap.empty[Pair,Int])(seqOp,combOp)    

    /* Setup transition matrix and add pair support*/  	
    val dim = states.length
    
    val matrix = new TransitionMatrix(dim,dim)
    matrix.setScale(scale)
    
    matrix.setStates(states, states)    
    for ((pair,support) <- pairsupp) {
      matrix.add(pair.ant, pair.con, support)
    }
            
    /* Normalize the matrix content and transform support into probabilities */
	matrix.normalize()

    matrix
    
  }
  
}