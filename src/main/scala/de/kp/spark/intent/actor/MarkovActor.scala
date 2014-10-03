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

import akka.actor.Actor

import org.apache.spark.rdd.RDD

import de.kp.spark.intent.{Configuration}

import de.kp.spark.intent.model._
import de.kp.spark.intent.redis.RedisCache

import de.kp.spark.intent.markov.MarkovBuilder
import de.kp.spark.intent.source.{PurchaseSource,Source}

import scala.collection.JavaConversions._

class MarkovActor extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("MarkovActor",Configuration.spark)      

  private def intents = Array(Intents.PURCHASE)
  
  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")     
      val task = req.task

      val params = properties(req)
      val missing = (params == null)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        RedisCache.addStatus(uid,task,IntentStatus.STARTED)
 
        try {

          buildModel(uid,task,req.data,params)
          
        } catch {
          case e:Exception => RedisCache.addStatus(uid,task,IntentStatus.FAILURE)          
        }

      }
      
      sc.stop
      context.stop(self)
          
    }
    
    case _ => {
      
      sc.stop
      context.stop(self)
      
    }
    
  }
   
  private def buildModel(uid:String,task:String,data:Map[String,String],intent:String) {
 
    intent match {
            
      case Intents.PURCHASE => {
              
        val source = new PurchaseSource(sc)
        val dataset = source.get(data)

        RedisCache.addStatus(uid,task,IntentStatus.DATASET)
        
        val scale = source.scaleDef
        val states = source.stateDefs

        val model = MarkovBuilder.build(scale,states,dataset)
        model.normalize
    
        /* Put model to cache */
        RedisCache.addModel(uid,model.serialize)
          
        /* Update cache */
        RedisCache.addStatus(uid,task,IntentStatus.FINISHED)

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
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_INTENT(uid))
      new ServiceResponse(req.service,req.task,data,IntentStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.MODEL_BUILDING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,IntentStatus.STARTED)	
  
    }

  }
  
}