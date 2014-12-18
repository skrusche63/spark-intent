package de.kp.spark.intent.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.core.model._

case class Purchase(site:String,user:String,timestamp:Long,amount:Float)
case class Purchases(items:List[Purchase])

object Serializer extends BaseSerializer {
  
  def serializePurchases(purchases:Purchases):String = write(purchases)
  def deserializePurchases(purchases:String):Purchases = read[Purchases](purchases)
  
}

object Algorithms {
  
  val MARKOV:String        = "MARKOV"
  val HIDDEN_MARKOV:String = "HIDDEN_MARKOV"

  private def algorithms = List(MARKOV,HIDDEN_MARKOV)  
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
}

object Intents {
  
  val LOYALTY:String  = "LOYALTY"
  val PURCHASE:String = "PURCHASE"
  /*
   * This is an internally used intent to indicate that
   * direct state oriented processing is required
   */
  val STATE:String = "STATE"
    
}

object Sources {
  /* The names of the data source actually supported */
  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PARQUET:String = "PARQUET"    
  val PIWIK:String   = "PIWIK"  
    
  private val sources = List(FILE,ELASTIC,JDBC,PARQUET,PIWIK)
  def isSource(source:String):Boolean = sources.contains(source)
    
}

object Messages extends BaseMessages {
  
  def MODEL_BUILDING_STARTED(uid:String):String = 
    String.format("""[UID: %s] Intent building started.""", uid)
  
  def MISSING_INTENT(uid:String):String = 
    String.format("""[UID: %s] Intent is missing.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = 
    String.format("""[UID: %s] Parameters are missing.""", uid)
  
  def MISSING_PURCHASES(uid:String):String = 
    String.format("""[UID: %s] Purchase is missing.""", uid)
  
  def MODEL_DOES_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] Model does not exist.""", uid)
  
}

object IntentStatus extends BaseStatus