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

import de.kp.spark.intent.{LoyaltyIntent,PurchaseIntent}

import de.kp.spark.intent.model._
import de.kp.spark.intent.redis.RedisCache

class ModelQuestor extends BaseActor {

  implicit val ec = context.dispatcher
  
  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task match {

        case "get:loyalty" => {

          val resp = if (RedisCache.modelExists(uid) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {    

             val prediction = new LoyaltyIntent().predict(uid,req.data)
                
             val data = Map("uid" -> uid, "loyalty" -> prediction)
             new ServiceResponse(req.service,req.task,data,IntentStatus.SUCCESS)
          
          }
           
          origin ! Serializer.serializeResponse(resp)
          context.stop(self)
          
        }
    
        case "get:purchase" => {

          val resp = if (RedisCache.modelExists(uid) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {   
            
            val prediction = new PurchaseIntent().predict(uid,req.data)

            val data = Map("uid" -> uid, "purchase" -> prediction)
            new ServiceResponse(req.service,req.task,data,IntentStatus.SUCCESS)
          
          }
          
        }
        
        case _ => {
      
          val origin = sender               
          val msg = Messages.REQUEST_IS_UNKNOWN()          
          
          origin ! Serializer.serializeResponse(failure(null,msg))
          context.stop(self)

        }
        
      }
      
    }    
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! Serializer.serializeResponse(failure(null,msg))
      context.stop(self)

    }
    
  }
   
}