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

import java.util.Date
import akka.actor.{Actor,ActorLogging}

import de.kp.spark.intent.{Configuration}

import de.kp.spark.intent.model._
import de.kp.spark.intent.redis.RedisCache

import de.kp.spark.intent.markov.HiddenMarkovTrainer
import de.kp.spark.intent.source.LoyaltySource

import scala.collection.JavaConversions._

class HiddenMarkovActor(@transient val sc:SparkContext) extends Actor with ActorLogging {

  private val base = Configuration.markov  
  private val intents = Array(Intents.LOYALTY)

  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")     
      val task = req.task

      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == 0.0))

      if (params != null) {
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
   
  private def buildModel(uid:String,task:String,data:Map[String,String],params:(String,Int,Double)) {
 
    val (intent,iterations,epsilon) = params
    intent match {
            
      case Intents.LOYALTY => {
              
        val source = new LoyaltySource(sc)
        val dataset = source.get(data)

        RedisCache.addStatus(uid,task,IntentStatus.DATASET)
        
        val states = source.stateDefs
        val hidden = source.hiddenDefs

        val model = HiddenMarkovTrainer.train(hidden,states,dataset,epsilon,iterations)

        val now = new Date()
        val dir = base + "/hmm-" + now.getTime().toString
    
        /* Save model in directory of file system */
        model.save(dir)
    
        /* Put model to cache */
        RedisCache.addModel(uid,dir)
          
        /* Update cache */
        RedisCache.addStatus(uid,task,IntentStatus.FINISHED)
        
      }
      
      case _ => { /* do nothing */}
      
    }
  
  }
  
  private def properties(req:ServiceRequest):(String,Int,Double) = {
      
    try {
      
      val epsilon = req.data("epsilon").asInstanceOf[Double]
      val iterations = req.data("iterations").asInstanceOf[Int]
      
      val intent = req.data("intent")
      if (intents.contains(intent)) {
        return (intent,iterations,epsilon)

      } else 
        return null
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,IntentStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.MODEL_BUILDING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,IntentStatus.STARTED)	
  
    }

  }
  
}