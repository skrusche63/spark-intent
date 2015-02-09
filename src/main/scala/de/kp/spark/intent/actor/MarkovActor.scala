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
import org.apache.spark.SparkContext._

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.source.StateSource
import de.kp.spark.core.source.handler.StateHandler

import de.kp.spark.intent.RequestContext
import de.kp.spark.intent.spec.StateSpec

import de.kp.spark.intent.model._
import de.kp.spark.intent.sink.RedisSink

import de.kp.spark.intent.markov._
import scala.collection.mutable.Buffer

class MarkovActor(@transient ctx:RequestContext) extends BaseActor {

  import ctx.sqlc.createSchemaRDD
  private val sink = new RedisSink()
  
  def receive = {
    
    case req:ServiceRequest => {

      val missing = (properties(req) == false)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        cache.addStatus(req,IntentStatus.MODEL_TRAINING_FINISHED)
 
        try {

          train(req)
          
        } catch {
          case e:Exception => cache.addStatus(req,IntentStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
   
  private def train(req:ServiceRequest) {

    val source = new StateSource(ctx.sc,ctx.config,new StateSpec(req))
    val dataset = StateHandler.state2Behavior(source.connect(req))
        
    val scale = req.data(Names.REQ_SCALE).toInt
    val states = req.data(Names.REQ_STATES).split(",")
    
    val matrix = new MarkovTrainer(scale,states).build(dataset)
    
    /*
     * We distinguish explicit and implicit train requests: 
     * an EXPLICIT request trains a certain transition matrix 
     * and does not apply this matrix to the provided dataset. 
     * 
     * The computation of next most probable states is done within 
     * an explicit query request. 
     * 
     * An IMPLICIT request apply the transition matrix directly to 
     * the dataset provided and determines for each last state of 
     * the user behavior the most probable next states, a number of 
     * steps ahead 
     */
    if (req.data.contains(Names.REQ_STEPS)) {
      /*
       * This request indicates an IMPLICIT request, and the number 
       * of steps specifies how many steps we have to look into the 
       * future and determine most probable states, starting from the 
       * last state in the user behavior
       */
      val steps = req.data(Names.REQ_STEPS).toInt
      require(steps > 0)

      /*
       * Determine the distint number of last states from the user behavior
       * provided with the dataset
       */
      val last_states = dataset.map(x => x.states.last).collect.distinct
      val table = ctx.sc.parallelize(last_states.flatMap(last_state => {
          
        val next_states = Buffer.empty[MarkovState]
        next_states += nextMarkovState(last_state,states,matrix)
    
        if (steps > 1) {
          (1 until steps.toInt).foreach(step => {
        
            val prev = next_states(step-1).name
            next_states += nextMarkovState(prev,states,matrix)
        
          })
        }
        next_states.zipWithIndex.map(v => {
          
          val (next_state,step) = v
          ParquetMSP(last_state,step,next_state.name,next_state.probability)
          
        })
            
      }))
    
      /* url = e.g.../part1/part2/part3/1 */
      val url = req.data(Names.REQ_URL)
   
      val pos = url.lastIndexOf('/')
    
      val base = url.substring(0, pos)
      val step = url.substring(pos+1).toInt + 1
    
      val store = base + "/" + (step + 1)    
      table.saveAsParquetFile(store)  
      
    }
    
    /* Put model to sink */
    val model = new MarkovSerializer().serialize(scale,states,matrix)
    sink.addModel(req,model)
          
    /* Update cache */
    cache.addStatus(req,IntentStatus.MODEL_TRAINING_FINISHED)

    /* Notify potential listeners */
    notify(req,IntentStatus.MODEL_TRAINING_FINISHED)
    
  }
  
  private def properties(req:ServiceRequest):Boolean = req.data.contains("intent")

  private def nextMarkovState(state:String,states:Array[String],matrix:TransitionMatrix):MarkovState = {
     
    /* Compute index of state from predefined states */
    val row = states.indexOf(state)
          
    /* Determine most probable next state from model */
    val probabilities = matrix.getRow(row)
    val max = probabilities.max
    
    val col = probabilities.indexOf(max)
    val next = states(col)
    
    MarkovState(next,max)
    
  }
  
}