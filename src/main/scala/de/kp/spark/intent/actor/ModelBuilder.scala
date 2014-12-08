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

import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.intent.Configuration
import de.kp.spark.intent.model._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class ModelBuilder(@transient sc:SparkContext) extends BaseTrainer(Configuration) {

  override def validate(req:ServiceRequest):Option[String] = {

    val uid = req.data(Names.REQ_UID)
    
    if (cache.statusExists(req)) {            
      return Some(Messages.TASK_ALREADY_STARTED(uid))   
    }
            
    req.data.get(Names.REQ_ALGORITHM) match {
        
      case None => {
        return Some(Messages.NO_ALGORITHM_PROVIDED(uid))              
      }
        
      case Some(algorithm) => {
        if (Algorithms.isAlgorithm(algorithm) == false) {
          return Some(Messages.ALGORITHM_IS_UNKNOWN(uid,algorithm))    
        }
          
      }
    
    }  
    
    req.data.get(Names.REQ_SOURCE) match {
        
      case None => {
        return Some(Messages.NO_SOURCE_PROVIDED(uid))       
      }
        
      case Some(source) => {
        if (Sources.isSource(source) == false) {
          return Some(Messages.SOURCE_IS_UNKNOWN(uid,source))    
        }          
      }
        
    }

    None
    
  }

  override def actor(req:ServiceRequest):ActorRef = {

    val algorithm = req.data(Names.REQ_ALGORITHM)
    if (algorithm == Algorithms.HIDDEN_MARKOV) {  
      context.actorOf(Props(new HiddenMarkovActor(sc)))      
      
    } else {
      context.actorOf(Props(new MarkovActor(sc)))      
    
    }
    
  }
  
}