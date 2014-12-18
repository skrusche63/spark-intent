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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.intent.model._
import de.kp.spark.intent.source._

import de.kp.spark.intent.sample._

class MarkovSource(@transient sc:SparkContext) {

  /*
   * A Markov source provides a scale factor, the respective Markov states 
   * and the user behavior, the model is derived from.
   */
  def getAsBehavior(req:ServiceRequest):(Int,Array[String],RDD[Behavior]) = {
    
    val intent = req.data(Names.REQ_INTENT)
    intent match {

      case Intents.PURCHASE => {
              
        val source = new PurchaseSource(sc)
        val dataset = source.get(req)
        
        val scale  = source.scale
        val states = source.ostates
      
        (scale,states,dataset)
        
      }
      /*
       * A 'state' intent retrieves the user behavior from a state specification; 
       * no additional transformation is required. This supports more generic training
       * requests, where the application logic is outside the Intent Recognition engine
       */
      case Intents.STATE => {

        val source = new StateSource(sc)
        val dataset = source.getAsBehavior(req)
        
        val scale = req.data(Names.REQ_SCALE).toInt
        val states = req.data(Names.REQ_STATES).split(",")
      
        (scale,states,dataset)
        
      }
       
      case _ => throw new Exception("Unknown intent.")
    
      
    }
 
  }

  def getAsObservation(req:ServiceRequest):(Array[String],Array[String],Array[String]) = {
     
    val intent = req.data(Names.REQ_INTENT)
    intent match {

      case Intents.LOYALTY => {
              
        val source = new LoyaltySource(sc)
        val dataset = source.get(req)
        
        val ostates = source.ostates
        val hstates = source.hstates

        (hstates,ostates,dataset)
        
      }
      /*
       * A 'state' intent retrieves the user behavior from a state specification; 
       * no additional transformation is required. This supports more generic training
       * requests, where the application logic is outside the Intent Recognition engine
       */
      case Intents.STATE => {

        val source = new StateSource(sc)
        val dataset = source.getAsObservation(req)
        
        val hstates = req.data(Names.REQ_HSTATES).split(",")
        val ostates = req.data(Names.REQ_OSTATES).split(",")

        (hstates,ostates,dataset)
        
      }
      case _ => throw new Exception("Unknown intent.")
    
      
    }

  }

}