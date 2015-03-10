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
import scala.collection.mutable.ArrayBuffer

class HiddenMarkovActor(@transient ctx:RequestContext) extends TrainActor(ctx) {

  private val base = ctx.config.markov  
  
  override def validate(req:ServiceRequest) {

    if (req.data.contains("name") == false) 
      throw new Exception("No name for Hidden Markov model provided.")
    
    if (req.data.contains("epsilon") == false)
      throw new Exception("Parameter 'epsilon' is missing.")

    if (req.data.contains("iterations") == false)
      throw new Exception("Parameter 'iterations' is missing.")
   
  }
   
  override def train(req:ServiceRequest) {
    
    val source = new StateSource(ctx.sc,ctx.config,new StateSpec(req))
    val dataset = StateHandler.state2Observation(source.connect(req))

    val name = req.data(Names.REQ_NAME) 
      
    val params = ArrayBuffer.empty[Param]

    val epsilon = req.data("epsilon").toDouble
    params += Param("epsilon","double",epsilon.toString)

    val iterations = req.data("iterations").toInt
    params += Param("iterations","integer",iterations.toString)

    cache.addParams(req, params.toList)
       
    val hstates = req.data(Names.REQ_HSTATES).split(",")
    val ostates = req.data(Names.REQ_OSTATES).split(",")
 
    val model = HiddenMarkovTrainer.train(hstates,ostates,dataset,epsilon,iterations)
    
    val now = new java.util.Date()
    val dir = String.format("""%s/model/%s/%s""",base,name,now.getTime().toString)
    
    /* Save model in directory of file system */
    model.save(dir)
    
    /* Put model to sink */
    redis.addModel(req,dir)
 
  }
  
}