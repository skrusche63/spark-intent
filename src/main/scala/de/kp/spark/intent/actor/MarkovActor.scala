package de.kp.spark.intent.actor
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

import de.kp.spark.core.model._
import de.kp.spark.intent.model._

import de.kp.spark.intent.sink.RedisSink

import de.kp.spark.intent.markov.MarkovBuilder
import de.kp.spark.intent.source.MarkovSource

class MarkovActor(@transient sc:SparkContext) extends BaseActor {

  private val sink = new RedisSink()
  
  def receive = {
    
    case req:ServiceRequest => {

      val missing = (properties(req) == false)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        cache.addStatus(req,IntentStatus.MODEL_TRAINING_FINISHED)
 
        try {

          buildModel(req)
          
        } catch {
          case e:Exception => cache.addStatus(req,IntentStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
   
  private def buildModel(req:ServiceRequest) {
 
    val (scale,states,dataset) = new MarkovSource(sc).getAsBehavior(req)
    val model = new MarkovBuilder(scale,states).build(dataset)
    
    /* Put model to sink */
    sink.addModel(req,model.serialize)
          
    /* Update cache */
    cache.addStatus(req,IntentStatus.MODEL_TRAINING_FINISHED)

    /* Notify potential listeners */
    notify(req,IntentStatus.MODEL_TRAINING_FINISHED)
    
  }
  
  private def properties(req:ServiceRequest):Boolean = req.data.contains("intent")
  
}