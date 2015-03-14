package de.kp.spark.intent.model

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.redis.RedisDB
import de.kp.spark.intent.{Configuration,MarkovIO}

import de.kp.spark.intent.markov._
import de.kp.spark.intent.model._

import scala.collection.mutable.ArrayBuffer

class MarkovPredictor(topic:String) extends IntentPredictor {

  private val (host,port) = Configuration.redis
  private val redis = new RedisDB(host,port.toInt)

  def predict(req:ServiceRequest):String = {
    
    topic match {
      /*
       * The topic 'observation' supports the retrieval of a sequence
       * of hidden states from a provided sequence of observations
       */
      case "observation" => {
    
        val store = redis.model(req)    
        val model = MarkovIO.readHM(store)
        /*
         * The Hidden Markov Predictor is flexible with respect to the
         * provided combination of request parameters
         */
        if (req.data.contains(Names.REQ_OBSERVATION)) {
          /*
           * Retrieve a sequence of (hidden) states from the 
           * provided observation
           */
          val observation = req.data(Names.REQ_OBSERVATION).split(",")
          val states = model.predict(observation)
        
          states.mkString(",")
          
        } else if (req.data.contains(Names.REQ_OBSERVATIONS)) {
          /*
           * Retrieve a list of sequences of (hidden) states
           * from the provided observations
           */
          val observations = req.data(Names.REQ_OBSERVATION).split(";").map(x => x.split(","))
          val states = observations.map(model.predict(_))
          
          states.map(x => x.mkString(",")).mkString(";")
          
          
        } else {
          throw new Exception("[Intent Recognition] The request parameters are not supported.")
        }
        
      }
      /*
       * The topic 'state' supports the retrieval of the most
       * probable sequence of subsequent states, starting from
       * a certain (provided) state
       */
      case "state" => {
    
        val store = redis.path(req)
        val (scale,states,matrix) = MarkovIO.readTM(store)

        /*
         * The number of steps, we have to look into the future, starting
         * from the provided state
         */
        val steps = req.data(Names.REQ_STEPS).toInt
        
        /*
         * The Markov predictor is flexible with respect to the provided
         * combination of request parameters: 
         * 
         */
        if (req.data.contains(Names.REQ_STATE)) {
          /*
           * A single (last) state is provided, and starting from this
           * state a set of most probable next states is computed
           */
          val state = req.data(Names.REQ_STATE).toString
    
          val markovStates = ArrayBuffer.empty[MarkovState]
          markovStates += nextMarkovState(state,states,matrix)
    
          if (steps > 1) {
            (1 until steps.toInt).foreach(step => {
        
              val prev = markovStates(step-1).name
              markovStates += nextMarkovState(prev,states,matrix)
        
            })
          }
          
          val rules = MarkovRules(List(MarkovRule(state,markovStates.toList)))
          Serializer.serializeMarkovRules(rules)
        
        } else if (req.data.contains(Names.REQ_STATES)) {
          /*
           * A list of (latest) states is provided, and for each state,
           * a set of most probable next states is computed
           */
          val states = req.data(Names.REQ_STATES).split(",")
          val rules = states.map(state => {
          
            val markovStates = ArrayBuffer.empty[MarkovState]
            markovStates += nextMarkovState(state,states,matrix)
    
            if (steps > 1) {
              (1 until steps.toInt).foreach(step => {
        
                val prev = markovStates(step-1).name
                markovStates += nextMarkovState(prev,states,matrix)
        
              })
            }
          
            MarkovRule(state,markovStates.toList)
            
          })
          
          Serializer.serializeMarkovRules(MarkovRules(rules.toList))
          
        } else {
          throw new Exception("[Intent Recognition] The request parameters are not supported.")
        }
       
      }
      
      case _ => throw new Exception("[Intent Recognition] This topic is not supported.")
    }
    
  }
  
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