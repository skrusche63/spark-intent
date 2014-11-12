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

import de.kp.spark.intent.{Configuration}

import de.kp.spark.intent.model._
import de.kp.spark.intent.redis.RedisCache

import de.kp.spark.intent.markov.MarkovBuilder
import de.kp.spark.intent.source.{PurchaseSource,Source}

import scala.collection.JavaConversions._
/*
 * The SparkContext is used to read data from different data source
 * and provide them as RDDs; building the markov model is independent
 * of Spark 
 */
class MarkovActor(@transient val sc:SparkContext) extends BaseActor {

  private def intents = Array(Intents.PURCHASE)
  
  def receive = {
    
    case req:ServiceRequest => {

      val params = properties(req)
      val missing = (params == null)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        RedisCache.addStatus(req,IntentStatus.STARTED)
 
        try {

          buildModel(req,req.data,params)
          
        } catch {
          case e:Exception => RedisCache.addStatus(req,IntentStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
   
  private def buildModel(req:ServiceRequest,data:Map[String,String],intent:String) {
 
    intent match {
            
      case Intents.PURCHASE => {
              
        val source = new PurchaseSource(sc)
        val dataset = source.get(data)

        RedisCache.addStatus(req,IntentStatus.DATASET)
        
        val scale = source.scaleDef
        val states = source.stateDefs

        val model = MarkovBuilder.build(scale,states,dataset)
        model.normalize
    
        /* Put model to cache */
        RedisCache.addModel(req,model.serialize)
          
        /* Update cache */
        RedisCache.addStatus(req,IntentStatus.FINISHED)

        /* Notify potential listeners */
        notify(req,IntentStatus.FINISHED)

      }
      
      case _ => { /* do nothing */}
      
    }
    
  }
  
  private def properties(req:ServiceRequest):String = {
      
    try {
      
      val intent = req.data("intent")
      return if (intents.contains(intent)) intent else null
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
}