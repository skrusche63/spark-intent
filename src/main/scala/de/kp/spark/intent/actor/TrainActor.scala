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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.redis.RedisDB
import de.kp.spark.intent.RequestContext

import de.kp.spark.intent.model._

abstract class TrainActor(@transient ctx:RequestContext) extends BaseActor {

  private val (host,port) = ctx.config.redis
  protected val redis = new RedisDB(host,port.toInt)            
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender
      val missing = try {
        
        validate(req)
        false
      
      } catch {
        case e:Exception => true
        
      }

      origin ! response(req, missing)

      if (missing == false) {
 
        try {

          /* Update cache */
          cache.addStatus(req,IntentStatus.MODEL_TRAINING_STARTED)
          
          train(req)
          
          /* Update cache */
          cache.addStatus(req,IntentStatus.MODEL_TRAINING_FINISHED)
 
        } catch {
          case e:Exception => cache.addStatus(req,IntentStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("unknown request.")
      context.stop(self)
      
    }
    
  }
  
  protected def validate(req:ServiceRequest)
  
  protected def train(req:ServiceRequest)

}