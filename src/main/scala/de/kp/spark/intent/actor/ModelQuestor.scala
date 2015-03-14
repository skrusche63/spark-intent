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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.redis.RedisDB
import de.kp.spark.intent.{Configuration,MarkovIO}

import de.kp.spark.intent.model._

class ModelQuestor extends BaseActor {

  implicit val ec = context.dispatcher

  private val (host,port) = Configuration.redis
  private val redis = new RedisDB(host,port.toInt)
  
  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)

      val Array(task,topic) = req.task.split(":")
      val response = try {
        
        if (redis.pathExists(req) == false) {           
          failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
        } else {
          
          val predictor = PredictorFactory.getPredictor(topic)
          val prediction = predictor.predict(req)
                
          val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> prediction)
          new ServiceResponse(req.service,req.task,data,IntentStatus.SUCCESS)
          
        }

      } catch {
        case e:Exception => failure(req,e.getMessage)
        
      }
           
      origin ! response
      context.stop(self)
      
    }    
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
    
  }
   
}