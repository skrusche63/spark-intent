package de.kp.spark.intent.model

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.intent.markov._
import de.kp.spark.intent.model._

import de.kp.spark.intent.sink.RedisSink
import scala.collection.mutable.ArrayBuffer

class MarkovPredictor(topic:String) extends IntentPredictor {
  
  private val sink = new RedisSink()

  def predict(req:ServiceRequest):String = {
    
    topic match {
      /*
       * The topic 'observation' supports the retrieval of a sequence
       * of hidden states from a provided sequence of observations
       */
      case "observation" => {
    
        val path = sink.model(req)    
        
        val model = new HiddenMarkovModel()
        model.load(path)
        /*
         * Retrieve a sequence of (hidden) states from the 
         * provided observations
         */
        val observations = req.data(Names.REQ_OBSERVATIONS).split(",")
        val states = model.predict(observations)
        
        states.mkString(",")
        
      }
      /*
       * The topic 'state' supports the retrieval of the most
       * probable sequence of subsequent states, starting from
       * a certain (provided) state
       */
      case "state" => {
    
        val model = sink.model(req)
        val (scale,states,matrix) = new MarkovSerializer().deserialize(model)

        /*
         * The number of steps, we have to look into the future, starting
         * from the provided state
         */
        val steps = req.data(Names.REQ_STEPS).toInt
        val state = req.data(Names.REQ_STATE).toString
    
        val markovStates = ArrayBuffer.empty[MarkovState]
        markovStates += nextMarkovState(state,states,matrix)
    
        if (steps > 1) {
          (1 until steps.toInt).foreach(step => {
        
            val prev = markovStates(step-1).name
            markovStates += nextMarkovState(prev,states,matrix)
        
          })
        }
    
        Serializer.serializeMarkovStates(MarkovStates(markovStates.toList))
        
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