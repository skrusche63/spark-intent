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

import akka.actor.{Actor,ActorLogging}

import de.kp.spark.intent.PurchaseIntent

import de.kp.spark.intent.model._
import de.kp.spark.intent.redis.RedisCache

class ModelQuestor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {
    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task match {

        case "get" => {

          val resp = if (RedisCache.modelExists(uid) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {    
               
            req.data.get("intent") match {
              
              case None => failure(req,Messages.MISSING_INTENT(uid))
              
              case Some(intent) => {
                
                try {

                  val prediction = predict(uid,intent,req.data)
                
                  val data = Map("uid" -> uid, "prediction" -> prediction)
                  new ServiceResponse(req.service,req.task,data,IntentStatus.SUCCESS)
              
                } catch {
                  case e:Exception => failure(req,e.getMessage())

                }
                
              }
             
            }
          
          }
           
          origin ! Serializer.serializeResponse(resp)
          
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          origin ! Serializer.serializeResponse(failure(req,msg))
           
        }
        
      }
      
    }    
  }

  private def predict(uid:String,intent:String,data:Map[String,String]):String = {
    
    intent match {
      
      case Intents.PURCHASE => new PurchaseIntent().predict(uid,data)
      
      case _ => "{}"
    
    }
    
  }
  
  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,IntentStatus.FAILURE)	
    
  }
  
}