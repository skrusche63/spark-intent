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

import de.kp.spark.intent.Configuration
import de.kp.spark.intent.model._

import de.kp.spark.intent.markov.StateSpec
import de.kp.spark.intent.spec.PurchaseSpec

import scala.collection.mutable.ArrayBuffer

class PurchaseModel() extends StateSpec with Serializable {
  
  private val spec = PurchaseSpec.get
  
  /*
   * Time based settings
   */
  protected val SMALL_TIME_THRESHOLD  = spec("time.threshold.small").toInt
  protected val MEDIUM_TIME_THRESHOLD = spec("time.threshold.medium").toInt
  
  protected val SMALL_TIME_HORIZON  = spec("time.horizon.small").toInt
  protected val MEDIUM_TIME_HORIZON = spec("time.horizon.equal").toInt
  protected val LARGE_TIME_HORIZON  = spec("time.horizon.large").toInt
  
  /*
   * Amount based settings
   */
  protected val LESS_AMOUNT_THRESHOLD  = spec("amount.threshold.less").toDouble
  protected val EQUAL_AMOUNT_THRESHOLD = spec("amount.threshold.equal").toDouble
  
  protected val LESS_AMOUNT_HORIZON  = spec("amount.horizon.less").toDouble
  protected val EQUAL_AMOUNT_HORIZON = spec("amount.horizon.equal").toDouble
  protected val LARGE_AMOUNT_HORIZON = spec("amount.horizon.large").toDouble
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  
  val FD_SCALE = 1
  val FD_STATE_DEFS = Array("SL", "SE", "SG", "ML", "ME", "MG", "LL", "LE", "LG")
  
  override def scaleDef = FD_SCALE
  
  override def stateDefs = FD_STATE_DEFS
  
  /**
   * Represent transactions as a time ordered sequence of Markov States;
   * the result is directly used to build the respective Markov Model
   */
  def buildStates(sequences:RDD[Purchase]):RDD[Behavior] = {
    
    /*
     * Group purchases by site & user and restrict to those
     * users with more than one purchase
     */
    sequences.groupBy(p => (p.site,p.user)).filter(_._2.size > 1).map(p => {

      val (site,user) = p._1
      val orders      = p._2.map(v => (v.timestamp,v.amount)).toList.sortBy(_._1)
      
      /* Extract first order */
      var (pre_time,pre_amount) = orders.head
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
      
      new Behavior(site,user,states.toList)
      
    })
    
  }
  /**
   * Amount spent compared to previous transaction
   * 
   * L : significantly less than
   * E : more or less same
   * G : significantly greater than
   * 
   */
  private def stateByAmount(next:Float,previous:Float):String = {
    
    if (next < LESS_AMOUNT_THRESHOLD * previous) "L"
     else if (next < EQUAL_AMOUNT_THRESHOLD * previous) "E"
     else "G"
    
  }
  /**   
   * This method translates a period of time, i.e. the time 
   * elapsed since last transaction into 3 discrete states:
   * 
   * S : small, M : medium, L : large
   * 
   */
  private def stateByTime(next:Long,previous:Long):String = {
    
    val period = (next -previous) / DAY
    
    if (period < SMALL_TIME_THRESHOLD) "S"
    else if (period < MEDIUM_TIME_THRESHOLD) "M"
    else "L"
  
  }
}