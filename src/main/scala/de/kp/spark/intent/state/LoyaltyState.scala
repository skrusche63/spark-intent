package de.kp.spark.intent.state
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

import de.kp.spark.intent.markov.StateSpec
import de.kp.spark.intent.spec.LoyaltySpec

class LoyaltyState extends StateSpec {
  
  private val spec = LoyaltySpec.get
  /*
   * Time based settings
   */
  protected val SMALL_TIME_HORIZON  = spec("time.horizon.small").toInt
  protected val MEDIUM_TIME_HORIZON = spec("time.horizon.equal").toInt
  protected val LARGE_TIME_HORIZON  = spec("time.horizon.large").toInt

  protected val SMALL_TIME_THRESHOLD  = spec("time.threshold.small").toInt
  protected val MEDIUM_TIME_THRESHOLD = spec("time.threshold.medium").toInt
  
  /*
   * Amount based settings
   */
  protected val LESS_AMOUNT_HORIZON  = spec("amount.horizon.less").toDouble
  protected val EQUAL_AMOUNT_HORIZON = spec("amount.horizon.equal").toDouble
  protected val LARGE_AMOUNT_HORIZON = spec("amount.horizon.large").toDouble
  
  protected val LESS_AMOUNT_THRESHOLD  = spec("amount.threshold.less").toDouble
  protected val EQUAL_AMOUNT_THRESHOLD = spec("amount.threshold.equal").toDouble
        
  protected val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  
  protected val O_STATE_DEFS = Array("SL", "SE", "SG", "ML", "ME", "MG", "LL", "LE", "LG")
  
  /*
   * The hidden states with respect to loyalty recognition are defined as
   * L (low), N (normal) and H (high)
   */
  protected val H_STATE_DEFS = Array("L","N","H")
  
  override def scaleDef:Int = {
    throw new Exception("Not implemented for Hidden Markov Models")
  }
  
  override def stateDefs = O_STATE_DEFS
  
  override def hiddenDefs = H_STATE_DEFS
  
  /**
   * Amount spent compared to previous transaction
   * 
   * L : significantly less than
   * E : more or less same
   * G : significantly greater than
   * 
   */
  protected def stateByAmount(next:Float,previous:Float):String = {
    
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
  protected def stateByTime(next:Long,previous:Long):String = {
    
    val period = (next -previous) / DAY
    
    if (period < SMALL_TIME_THRESHOLD) "S"
    else if (period < MEDIUM_TIME_THRESHOLD) "M"
    else "L"
  
  }

}