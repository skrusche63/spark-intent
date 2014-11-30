package de.kp.spark.intent.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Intent project
* (https://github.com/skrusche63/spark-cluster).
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

import de.kp.spark.core.model._
import de.kp.spark.intent.model._

import scala.collection.mutable.ArrayBuffer

class IntentRegistrar extends BaseActor {

  implicit val ec = context.dispatcher

  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      val uid = req.data("uid")
      
      req.task match {
        
        case "register:loyalty" => {
       
          val response = try {
        
            /* Unpack fields from request and register in Redis instance */
            val fields = ArrayBuffer.empty[Field]

            fields += new Field("site","string",req.data("site"))
            fields += new Field("timestamp","long",req.data("timestamp"))

            fields += new Field("user","string",req.data("user"))
            fields += new Field("amount","float",req.data("amount"))
            
            cache.addFields(req, new Fields(fields.toList))
        
            new ServiceResponse("intent","register",Map("uid"-> uid),IntentStatus.SUCCESS)
        
          } catch {
            case throwable:Throwable => failure(req,throwable.getMessage)
          }
      
          origin ! response
          
        } 
        
        case "register:purchase" => {
      
          val response = try {
        
            /* Unpack fields from request and register in Redis instance */
            val fields = ArrayBuffer.empty[Field]

            fields += new Field("site","string",req.data("site"))
            fields += new Field("timestamp","long",req.data("timestamp"))

            fields += new Field("user","string",req.data("user"))
            fields += new Field("amount","float",req.data("amount"))
            
            cache.addFields(req, new Fields(fields.toList))
        
            new ServiceResponse("intent","register",Map("uid"-> uid),IntentStatus.SUCCESS)
        
          } catch {
            case throwable:Throwable => failure(req,throwable.getMessage)
          }
      
          origin ! response
          
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
          origin ! failure(req,msg)
          context.stop(self)
          
        }
        
      }
    }
  
  }
}