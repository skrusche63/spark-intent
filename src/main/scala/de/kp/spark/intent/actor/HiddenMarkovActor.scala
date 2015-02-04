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

import de.kp.spark.core.source.StateSource
import de.kp.spark.core.source.handler.StateHandler

import de.kp.spark.intent.RequestContext
import de.kp.spark.intent.spec.StateSpec

import de.kp.spark.intent.model._
import de.kp.spark.intent.sink.RedisSink

import de.kp.spark.intent.markov.HiddenMarkovTrainer

class HiddenMarkovActor(@transient ctx:RequestContext) extends BaseActor {

  private val base = ctx.config.markov  
  private val sink = new RedisSink()
  
  def receive = {
    
    case req:ServiceRequest => {

      val missing = (properties(req) == false)
      sender ! response(req, missing)

      if (missing == false) {
 
        try {
            build(req)
          
        } catch {
          case e:Exception => cache.addStatus(req,IntentStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      log.error("Unknow request.")
      context.stop(self)     

    }
    
  }
   
  private def build(req:ServiceRequest) {
    /**
     * The training request must provide a name for the random forest 
     * to uniquely distinguish this forest from all others
     */
    val name = if (req.data.contains(Names.REQ_NAME)) req.data(Names.REQ_NAME) 
      else throw new Exception("No name for hidden markov model provided.")

    /* Register status */
    cache.addStatus(req,IntentStatus.MODEL_TRAINING_STARTED)

    val epsilon = req.data("epsilon").toDouble
    val iterations = req.data("iterations").toInt
    
    val source = new StateSource(ctx.sc,ctx.config,StateSpec)
    val dataset = StateHandler.state2Observation(source.connect(req))
       
    val hstates = req.data(Names.REQ_HSTATES).split(",")
    val ostates = req.data(Names.REQ_OSTATES).split(",")

    val model = HiddenMarkovTrainer.train(hstates,ostates,dataset,epsilon,iterations)
    
    val now = new java.util.Date()
    val dir = String.format("""%s/model/%s/%s""",base,name,now.getTime().toString)
    
    /* Save model in directory of file system */
    model.save(dir)
    
    /* Put model to sink */
    sink.addModel(req,dir)
          
    /* Update cache */
    cache.addStatus(req,IntentStatus.MODEL_TRAINING_FINISHED)

    /* Notify potential listeners */
    notify(req,IntentStatus.MODEL_TRAINING_FINISHED)
 
  }
  
  private def properties(req:ServiceRequest):Boolean = {
    
    if (req.data.contains("epsilon") == false) return false
    if (req.data.contains("iterations") == false) return false
    
    true
    
  }
  
}