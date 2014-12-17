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
        
        val scale = source.scaleDef
        val states = source.stateDefs
      
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
        
        val ostates = source.stateDefs
        val hstates = source.hiddenDefs

        (hstates,ostates,dataset)
        
      }
       
      case _ => throw new Exception("Unknown intent.")
    
      
    }

  }

}