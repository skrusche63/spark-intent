package de.kp.spark.intent
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.intent.markov.TransitionMatrix

import de.kp.spark.intent.model._
import de.kp.spark.intent.sink.RedisSink

import de.kp.spark.intent.state.PurchaseState
import scala.collection.mutable.ArrayBuffer

class PurchaseIntent extends PurchaseState {
  
  private val sink = new RedisSink()
  
  def predict(req:ServiceRequest):String = {
    
    val dim = FD_STATE_DEFS.length
	val matrix = new TransitionMatrix(dim,dim)
      
    matrix.setScale(FD_SCALE)
    matrix.setStates(FD_STATE_DEFS,FD_STATE_DEFS)

    val model = sink.model(req)
    matrix.deserialize(model)
    
    req.data.get("purchases") match {
      
      case None => throw new Exception(Messages.MISSING_PURCHASES(Names.REQ_UID))
      
      case Some(value) => {
        
        val purchases = Serializer.deserializePurchases(value).items
        /*
         * Group purchases by site & user and restrict to those
         * users with more than one purchase
         */
        val next_purchases = purchases.groupBy(p => (p.site,p.user)).filter(_._2.size > 1).map(p => {

          val (site,user) = p._1
          val orders      = p._2.map(v => (v.timestamp,v.amount)).toList.sortBy(_._1)
      
          /* Extract first order */
          var (pre_time,pre_amount) = orders.head
          
          /* Extract last order */
          val (last_time,last_amount) = orders.last
          val states = ArrayBuffer.empty[String]

          for ((time,amount) <- orders.tail) {
        
            /* Determine state from amount */
            val astate = stateByAmount(amount,pre_amount)
     
            /* Determine state from time elapsed between
             * subsequent orders or transactions
             */
            val tstate = stateByTime(time,pre_time)
      
            val state = astate + tstate
            states += state
        
            pre_amount = amount
            pre_time   = time
        
          }
      
          val last_state = states.last

          /* Compute index of last state from STATE_DEFS */
          val row = FD_STATE_DEFS.indexOf(last_state)
          /* Determine most probable next state from model */
          val probs = matrix.getRow(row)
          val maxProb = probs.max
    
          val col = probs.indexOf(maxProb)
          val next_state = FD_STATE_DEFS(col)

          val (next_time,next_amount) = (nextDate(next_state,last_time),nextAmount(next_state,last_amount))
          new Purchase(site,user,next_time,next_amount)
          
        }).toList
      
        Serializer.serializePurchases(new Purchases(next_purchases))
        
      }
      
    }

  }

}