package de.kp.scala.hmm
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Scala-HMM project
* (https://github.com/skrusche63/scala-hmm).
* 
* Scala-HMM is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Scala-HMM is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Scala-HMM. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import java.util.Random

import org.apache.mahout.common.RandomUtils
import org.apache.mahout.math.{DenseMatrix,DenseVector}

import scala.collection.mutable.HashMap

/**
 */
class HmmModel extends Cloneable {

  /* 
   * Map for storing the observed state names 
   */
  private var outputStateNames = HashMap.empty[String,Int] 
  /* 
   * Map for storing the hidden state names 
   */
  private var hiddenStateNames = HashMap.empty[String,Int]  

  /* Number of hidden states */
  private var numHiddenStates:Int = 0

  /* Number of output states */
  private var numOutputStates:Int = 0

  /*
   * Transition matrix containing the transition probabilities 
   * between hidden states. 
   * 
   * TransitionMatrix(i,j) is the probability that we change from 
   * hidden state i to hidden state j 
   * 
   * In general: P(h(t+1)=h_j | h(t) = h_i) = TransitionMatrix(i,j) 
   * 
   * Since we have to make sure that each hidden state can be "left", 
   * the following normalization condition has to hold:
   * 
   * sum(TransitionMatrix(i,j),j=1..hiddenStates) = 1
   */
  private var A:DenseMatrix = null

  /*
   * Output matrix containing the probabilities that we observe 
   * a given output state given a hidden state. 
   * 
   * OutputMatrix(i,j) is the probability that we observe output 
   * state j if we are in hidden state i. 
   * 
   * Formally: P(o(t)=o_j | h(t)=h_i) = outputMatrix(i,j) 
   * 
   * Since we always have an observation for each hidden state, 
   * the following normalization condition has to hold:
   * 
   * sum(OutputMatrix(i,j),j=1..outputStates) = 1
   */
  private var B:DenseMatrix = null

  /*
   * Vector containing the initial hidden state probabilities:
   * 
   * P(h(0)=h_i) = initialProbabilities(i) 
   * 
   * Since we are dealing with probabilities the following 
   * normalization condition has to hold:
   * 
   * sum(InitialProbabilities(i),i=1..hiddenStates) = 1
   */
  private var Pi:DenseVector = null


  /**
   * Clone the model
   */
  override def clone():HmmModel = {
    
    val AMatrixClone = A.clone().asInstanceOf[DenseMatrix]
    val BMatrixClone = B.clone().asInstanceOf[DenseMatrix]
    
    val model = new HmmModel(AMatrixClone, BMatrixClone, Pi.clone())
    if (hiddenStateNames != null) {
       model.hiddenStateNames = hiddenStateNames.clone()
    
    }
    if (outputStateNames != null) {
      model.outputStateNames = outputStateNames.clone()
   
    }
    
    model
  
  }

  /**
   * Assign the content of another HMM model 
   * to this model
   */
  def assign(model:HmmModel) {
    
    this.numHiddenStates = model.numHiddenStates;
    this.numOutputStates = model.numOutputStates;
    
    this.hiddenStateNames = model.hiddenStateNames;
    this.outputStateNames = model.outputStateNames;
    
    this.Pi = model.Pi.clone()
    
    this.A = model.A.clone().asInstanceOf[DenseMatrix]
    this.B = model.B.clone().asInstanceOf[DenseMatrix]
  
  }

  /**
   * Construct a valid random Hidden-Markov parameter set 
   * with the given number of hidden and output states using 
   * a given seed. 
   * 
   * Seed is for the random initialization, if set to 0 the 
   * current time is used.
   */
  def this(numHiddenStates:Int,numOutputStates:Int,seed:Long) {
    this()
    
    this.numHiddenStates = numHiddenStates
    this.numOutputStates = numOutputStates
    
    this.A = new DenseMatrix(numHiddenStates, numHiddenStates)
    this.B = new DenseMatrix(numHiddenStates, numOutputStates)
    
    this.Pi = new DenseVector(numHiddenStates)

    initRandomParameters(seed)
  
  }

  /**
   * Construct a valid random Hidden-Markov parameter set 
   * with the given number of hidden and output states.
   */
  def this(numHiddenStates:Int, numOutputStates:Int) {
    this(numHiddenStates, numOutputStates, 0);
  }

  /**
   * Generates a Hidden Markov model using specified parameters.
   */
  def this(A:DenseMatrix,B:DenseMatrix,Pi:DenseVector) {
    this()
    
    this.numHiddenStates = Pi.size()
    this.numOutputStates = B.numCols()
    
    this.A = A
    this.B = B
    
    this.Pi = Pi
  
  }

  /**
   * Initialize a valid random set of HMM parameters
   */
  private def initRandomParameters(seed:Long) {
    
    /* Initialize the random number generator */
    val rand:Random = if (seed == 0) RandomUtils.getRandom() else RandomUtils.getRandom(seed)
    
    /* Initialize the initial probabilities */
    var sum:Double = 0 
    (0 until numHiddenStates).foreach(i => {
      
      val nextRand = rand.nextDouble()
      Pi.set(i, nextRand)
    
      sum += nextRand
    
    })
    
    /* "Normalize" the vector to generate probabilities */
    Pi = Pi.divide(sum).asInstanceOf[DenseVector]

    /* Initialize the transition matrix */
    var values = Array.fill[Double](numHiddenStates)(0)
    (0 until numHiddenStates).foreach(i => {
      
      sum = 0
      (0 until numHiddenStates).foreach(j => {
        values(j) = rand.nextDouble()
        sum += values(j)
      })
      
      /* Normalize the random values to obtain probabilities */
      (0 until numHiddenStates).foreach(j => values(j) /= sum)
      
      /* Set this row of the transition matrix */
      A.set(i, values)
    
    })

    /* Initialize the output matrix */
    values = Array.fill[Double](numOutputStates)(0)
    (0 until numHiddenStates).foreach(i => {
      
      sum = 0
      (0 until numOutputStates).foreach(j => {
        values(j) = rand.nextDouble()
        sum += values(j)
      })
      
      /* Normalize the random values to obtain probabilities */
      (0 until numOutputStates).foreach(j => values(j) /= sum)
      
      /* Set this row of the output matrix */
      B.set(i, values)
      
    })
    
  }

  def getNumHiddenStates():Int = numHiddenStates

  def getNumOutputStates():Int = numOutputStates

  def getAMatrix:DenseMatrix = A

  def getBMatrix:DenseMatrix = B

  def getPiVector:DenseVector = Pi

  def getHiddenStateNames():Map[String,Int] = hiddenStateNames.toMap

  /**
   * Register an array of hidden state names. We assume that the 
   * state name at position i has the ID i
   */
  def registerHiddenStateNames(stateNames:Array[String]) {
    
    if (stateNames != null) {
      (0 until stateNames.length).foreach(i => hiddenStateNames += stateNames(i) -> i)    
    }
  
  }

  /**
   * Register a map of hidden state Names/state IDs
   */
  def registerHiddenStateNames(stateNames:Map[String,Int]) {
    
    if (stateNames != null) {
      
      for (stateName <- stateNames) {
        hiddenStateNames += stateName
      }
      
    }
  
  }

  /**
   * Lookup the name for the given hidden state ID
   */
  def getHiddenStateName(id:Int):String = {
    
    if (hiddenStateNames.isEmpty) return null
    
    val names = hiddenStateNames.filter(e => e._2 == id).map(e => e._1).toSeq
    if (names.size > 0) names(0) else null
    
  }

  /**
   * Lookup the ID for the given hidden state name
   */
  def getHiddenStateID(name:String):Int = {
    
    hiddenStateNames.get(name) match {
      case None => -1
      case Some(id) => id
    }
    
  }

  def getOutputStateNames():Map[String,Int] = outputStateNames.toMap

  /**
   * Register an array of output state names. We assume that the 
   * state name at position i has the ID i
   */
  def registerOutputStateNames(stateNames:Array[String]) {
    
    if (stateNames != null) {
            
      for (i <- 0 until stateNames.length) {
        outputStateNames += stateNames(i) -> i
      }
    
    }
  
  }

  /**
   * Register a map of hidden state Names/state IDs
   */
  def registerOutputStateNames(stateNames:Map[String,Int]) {
    if (stateNames != null) {
      
      for (stateName <- stateNames) {
        outputStateNames += stateName
      }
      
    }
  }

  /**
   * Lookup the name for the given output state id
   */
  def getOutputStateName(id:Int):String = {
    
    if (outputStateNames.isEmpty) return null
    
    val names = outputStateNames.filter(e => e._2 == id).map(e => e._1).toSeq
    if (names.size > 0) names(0) else null
  
  }

  /**
   * Lookup the ID for the given output state name
   */
  def getOutputStateID(name:String):Int = {
    
    outputStateNames.get(name) match {
      case None => -1
      case Some(id) => id
      
    }
  
  }

  /**
   * Normalize the probabilities of the model
   */
  def normalize() {
    
    var isum = 0.0
    (0 until numHiddenStates).foreach(i => {
      
      isum += Pi.getQuick(i)
      
      var sum = 0.0
      (0 until numHiddenStates).foreach(j => sum += A.getQuick(i,j))
      
      if (sum != 1.0) {
        (0 until numHiddenStates).foreach(j => A.setQuick(i,j, A.getQuick(i,j) / sum))
      }
      
      sum = 0.0
      (0 until numOutputStates).foreach(j => sum += B.getQuick(i,j))
      
      if (sum != 1.0) {
        (0 until numOutputStates).foreach(j => B.setQuick(i,j, B.getQuick(i,j) / sum))
      }
      
    })
    
    if (isum != 1.0) {
      (0 until numHiddenStates).foreach(i => Pi.setQuick(i, Pi.getQuick(i) / isum))      
    }
  }

}