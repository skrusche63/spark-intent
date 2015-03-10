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

import de.kp.spark.core.Names
import de.kp.spark.core.actor.RootActor

import de.kp.spark.core.model._

import de.kp.spark.intent.Configuration
import de.kp.spark.intent.model._

abstract class BaseActor extends RootActor(Configuration) {
  
  protected def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data(Names.REQ_UID)
    
    if (missing == true) {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MISSING_INTENT(uid))
      new ServiceResponse(req.service,req.task,data,IntentStatus.FAILURE)	
  
    } else {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MODEL_BUILDING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,IntentStatus.MODEL_TRAINING_STARTED)	
  
    }

  }

}