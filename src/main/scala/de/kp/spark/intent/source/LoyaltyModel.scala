package de.kp.spark.intent.source
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

import de.kp.spark.intent.model._

import de.kp.spark.intent.state.LoyaltyState
import scala.collection.mutable.ArrayBuffer

class LoyaltyModel() extends LoyaltyState with Serializable {
  
  /**
   * Represent transactions as a time ordered sequence of Markov States;
   * the result is directly used to build the respective Hidden Markov Model
   */
  def observations(purchases:RDD[Purchase]):Array[String] = {
    
    /*
     * Sort sequences of all purchases first by ascending (true)
     * timestamps to represent them as observations; to this end,
     * no knowledge about the respective site and user is relevant
     */
    val dataset = purchases.coalesce(1, false).sortBy(p => p.timestamp, true, 1)

    def seqOp(observations:Observations,purchase:Purchase):Observations = {

      observations.add(purchase)
      observations
      
    }
    /*
     * Note that observ1 is always NULL
     */
    def combOp(observ1:Observations,observ2:Observations):Observations = observ2      

    dataset.aggregate(new Observations())(seqOp,combOp).states.toArray    
  
  }
  
  private class Observations() {
    
    val states = ArrayBuffer.empty[String]
    
    var pre_purchase:Purchase = null
    
    def add(purchase:Purchase) {
      
      if (pre_purchase == null) {
        pre_purchase = purchase
      
      } else {
        
        val (pre_time,pre_amount) = (pre_purchase.timestamp,pre_purchase.amount)        
        val (time,amount) = (purchase.timestamp,purchase.amount)
        
        /* Determine state from amount */
        val astate = stateByAmount(amount,pre_amount)
     
        /* Determine state from time elapsed between
         * subsequent orders or transactions
         */
        val tstate = stateByTime(time,pre_time)
      
        val state = astate + tstate
        states += state

        pre_purchase = purchase
       
      }
      
    }
  }
}