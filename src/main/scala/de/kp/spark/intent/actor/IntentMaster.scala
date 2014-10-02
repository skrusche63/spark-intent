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

import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}
import akka.routing.RoundRobinRouter

import de.kp.spark.intent.Configuration
import de.kp.spark.intent.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class IntentMaster extends Actor with ActorLogging {
  
  /* Load configuration for routers */
  val (time,retries,workers) = Configuration.router   

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }

  val builder = context.actorOf(Props[ModelBuilder])
  val questor = context.actorOf(Props[ModelQuestor].withRouter(RoundRobinRouter(workers)))
  
  def receive = {
    
    case req:String => {
      
      implicit val ec = context.dispatcher

      val duration = Configuration.actor      
      implicit val timeout:Timeout = DurationInt(duration).second
	  	    
	  val origin = sender

	  val deser = Serializer.deserializeRequest(req)
	  val response = deser.task.split(":")(0) match {

	    case "get" => ask(questor,deser).mapTo[ServiceResponse]

	    case "train"  => ask(builder,deser).mapTo[ServiceResponse]

	    case "status" => ask(builder,deser).mapTo[ServiceResponse]
       
        case _ => {

          Future {     
            failure(deser,Messages.TASK_IS_UNKNOWN(deser.data("uid"),deser.task))
          } 
        
        }
      
      }
      response.onSuccess {
        case result => origin ! Serializer.serializeResponse(result)
      }
      response.onFailure {
        case result => origin ! failure(deser,Messages.GENERAL_ERROR(deser.data("uid")))	      
	  }
      
    }
  
    case _ => {}
    
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,IntentStatus.FAILURE)	
    
  }

}