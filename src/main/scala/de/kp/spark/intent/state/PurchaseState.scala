package de.kp.spark.intent.state

import de.kp.spark.intent.markov.StateSpec
import de.kp.spark.intent.spec.PurchaseSpec

class PurchaseState extends StateSpec {
  
  private val spec = PurchaseSpec.get
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
  
  protected val FD_SCALE = 1
  protected val FD_STATE_DEFS = Array("SL", "SE", "SG", "ML", "ME", "MG", "LL", "LE", "LG")
  
  override def scaleDef = FD_SCALE
  
  override def stateDefs = FD_STATE_DEFS
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
  
  protected def nextAmount(nextstate:String,lastamount:Float):Float = {
    
    if (nextstate == "") return 0
    
    lastamount * (
    
        if (nextstate.endsWith("L")) LESS_AMOUNT_HORIZON.toFloat         
        else if (nextstate.endsWith("E")) EQUAL_AMOUNT_HORIZON.toFloat    
        else LARGE_AMOUNT_HORIZON.toFloat
    
    )
    
  }

  protected def nextDate(nextstate:String,lastdate:Long):Long = {

    if (nextstate == "") return -1
    
    lastdate + DAY * (
    
        if (nextstate.startsWith("S")) SMALL_TIME_HORIZON      
        else if (nextstate.startsWith("M")) MEDIUM_TIME_HORIZON   
        else LARGE_TIME_HORIZON
        
    )

  }
  
}