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
/*
 * This class specifies a list of user states that are used to represent
 * the customers (purchase) behavior within a certain period of time 
 */
case class Behavior(site:String,user:String,states:List[String])
case class Behaviors(items:List[Behavior])

case class Purchase(site:String,user:String,timestamp:Long,amount:Float)
case class Purchases(items:List[Purchase])

object Serializer extends BaseSerializer {

  def serializeBehaviors(behaviors:Behaviors):String = write(behaviors)
  def deserializeBehavior(behaviors:String):Behaviors = read[Behaviors](behaviors)
  
  def serializePurchases(purchases:Purchases):String = write(purchases)
  def deserializePurchases(purchases:String):Purchases = read[Purchases](purchases)
  
}

object Algorithms {
  
  val MARKOV:String        = "MARKOV"
  val HIDDEN_MARKOV:String = "HIDDEN_MARKOV"
  
}

object Intents {
  
  val LOYALTY:String  = "LOYALTY"
  val PURCHASE:String = "PURCHASE"
    
}

object Sources {
  /* The names of the data source actually supported */
  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PIWIK:String   = "PIWIK"    
}

object Messages extends BaseMessages {
 
  def DATA_TO_TRACK_RECEIVED(uid:String):String = String.format("""Data to track received for uid '%s'.""", uid)
  
  def MODEL_BUILDING_STARTED(uid:String):String = String.format("""Intent building started for uid '%s'.""", uid)
  
  def MISSING_INTENT(uid:String):String = String.format("""Intent is missing for uid '%s'.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameters are missing for uid '%s'.""", uid)
  
  def MISSING_PURCHASES(uid:String):String = String.format("""Purchase is missing for uid '%s'.""", uid)
  
  def MODEL_DOES_NOT_EXIST(uid:String):String = String.format("""Model does not exist for uid '%s'.""", uid)

  def SEARCH_INDEX_CREATED(uid:String):String = String.format("""Search index created for uid '%s'.""", uid)
  
}

object IntentStatus extends BaseStatus {
  
  val DATASET:String = "dataset"
    
  val STARTED:String = "started"
  val FINISHED:String = "finished"
    
}