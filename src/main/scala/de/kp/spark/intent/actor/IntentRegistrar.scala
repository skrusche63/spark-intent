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

import de.kp.spark.core.Names
import de.kp.spark.core.spec.FieldBuilder

import de.kp.spark.core.model._
import de.kp.spark.intent.model._

class IntentRegistrar extends BaseActor {

  implicit val ec = context.dispatcher

  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      val uid = req.data(Names.REQ_UID)
       
      val response = try {
 
        req.task.split(":")(1) match {
        
          case "loyalty" => {
        
            val fields = new FieldBuilder().build(req,"amount")            
            cache.addFields(req, fields)
        
            new ServiceResponse(req.service,req.task,Map(Names.REQ_UID-> uid),IntentStatus.SUCCESS)
          
          }
           
          case "purchase" => {
        
            val fields = new FieldBuilder().build(req,"amount")            
            cache.addFields(req, fields)
        
            new ServiceResponse(req.service,req.task,Map(Names.REQ_UID-> uid),IntentStatus.SUCCESS)
          
          }

          case _ => {
          
            val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
            failure(req,msg)
          
          }
          
        }
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! response
      context.stop(self)       

    }
  
  }
}