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

/**
 * The HMMPredictor offers several methods to predict from a HMM Model. 
 * 
 * The following use-cases are covered: 
 * 
 * 1) Generate a sequence of output states from a given model (prediction). 
 * 
 * 2) Compute the likelihood that a given model generated a given sequence 
 *    of output states (model likelihood). 
 *    
 * 3) Compute the most likely hidden sequence for a given model and a given 
 *    observed sequence (decoding).
 */
object HmmPredictor {

  /**
   * Predict the most likely sequence of hidden states for the given model and
   * observation. Set scaled to true, if log-scaled computation is to be used.
   * 
   * This requires higher computational effort but is numerically more stable 
   * for large observation sequences.
   */
  def predict(model:HmmModel,observedStates:Array[Int],scaled:Boolean):Array[Int] = {
    AlgoViterbi.run(model,observedStates,scaled)
  }

  /**
   * Predict a sequence of steps output states for the given HMM model
   */
  def predict(model:HmmModel,steps:Int):Array[Int] = predict(model,steps,RandomUtils.getRandom())

  /**
   * Predict a sequence of steps output states for the given HMM model
   */
  def predict(model:HmmModel,steps:Int,seed:Long):Array[Int] = predict(model, steps, RandomUtils.getRandom(seed))
  
  /**
   * Predict a sequence of steps output states for the given HMM model 
   * using the given seed for probabilistic experiments
   */
  private def predict(model:HmmModel,steps:Int,rand:Random):Array[Int] = {
     
    val cA = getCumulativeA(model)
    val cB = getCumulativeB(model)

    val cPi = getCumulativePi(model)
    
    val outputStates = new Array[Int](steps)
    
    /* Choose the initial state */
    var hiddenState = 0

    var randnr = rand.nextDouble()
    while (cPi.get(hiddenState) < randnr) {
      hiddenState += 1
    }

    /* 
     * Draw steps output states according to the 
     * cumulative distributions
     */
    for (step <- 0 until steps) {
      /* Choose output state to given hidden state */
      randnr = rand.nextDouble()
      var outputState = 0
      
      while (cB.get(hiddenState, outputState) < randnr) {
        outputState += 1
      }
      
      outputStates(step) = outputState
      
      /* Choose the next hidden state */
      randnr = rand.nextDouble();
      var nextHiddenState = 0
      
      while (cA.get(hiddenState, nextHiddenState) < randnr) {
        nextHiddenState += 1
      }
      
      hiddenState = nextHiddenState
    }
    
    outputStates
  
  }
  
  /**
   * Compute the cumulative transition probability matrix for the 
   * given HMM model. Each row i is the cumulative distribution of 
   * the transition probability distribution for hidden state i.
   */
  private def getCumulativeA(model:HmmModel):DenseMatrix = {
    
    val numHiddenStates = model.getNumHiddenStates()
    
    val A = model.getAMatrix
    val cA = new DenseMatrix(numHiddenStates, numHiddenStates)
    
    (0 until numHiddenStates).foreach(i => {
      
      var sum = 0.0
      (0 until numHiddenStates).foreach(j => {
        sum += A.get(i,j)
        cA.set(i,j, sum)
      })
      /*
       * Make sure the last hidden state has always 
       * a cumulative probability of exactly 1.0      
       */
      cA.set(i, numHiddenStates - 1, 1.0)

    })
    
    cA
  
  }
  
  /**
   * Compute the cumulative output probability matrix for the 
   * given HMM model. Each row i is the cumulative distribution 
   * of the output probability distribution for hidden state i.
   */
  private def getCumulativeB(model:HmmModel):DenseMatrix = {
    
    val numHiddenStates = model.getNumHiddenStates()
    val numOutputStates = model.getNumOutputStates()
    
    val B = model.getBMatrix   
    val cB = new DenseMatrix(numHiddenStates, numOutputStates)

    (0 until numHiddenStates).foreach(i => {
      
      var sum = 0.0
      (0 until numOutputStates).foreach(j => {
        sum += B.get(i, j)
        cB.set(i, j, sum)
      })
      /*
       * Make sure the last output state has always 
       * a cumulative probability of exactly 1.0      
       */       
      cB.set(i, numOutputStates - 1, 1.0)
 
    })
    
    cB
  
  }

  /**
   * Compute the cumulative distribution of the initial hidden 
   * state probabilities for the given HMM model.
   */
  private def getCumulativePi(model:HmmModel):DenseVector = {
    
    val numHiddenStates = model.getNumHiddenStates()
    
    val Pi = model.getPiVector 
    val cPi = new DenseVector(Pi.size())
    
    var sum = 0.0    
    (0 until numHiddenStates).foreach(i => {
      sum += Pi.get(i)
      cPi.set(i, sum)
    })
    /*
     * Make sure the last initial hidden state has always 
     * a cumulative probability of exactly 1.0      
     */    
    cPi.set(numHiddenStates - 1, 1.0) // make sure the last initial
    cPi
    
  }

  /**
   * Returns the likelihood that a given output sequence was produced by the
   * given model. Internally, this function calls the forward algorithm to
   * compute the alpha values and then uses the overloaded function to compute
   * the actual model likelihood.
   * 
   * If scaled is set to true, log-scaled parameters are used for computation. 
   * This is computationally more expensive, but offers better numerically 
   * stability in case of long output sequences.
   */
  def modelLikelihood(model:HmmModel,outputStates:Array[Int],scaled:Boolean):Double = modelLikelihood(AlgoForward.run(model,outputStates,scaled),scaled)

  def modelLikelihood(alpha:DenseMatrix,scaled:Boolean):Double = {
   
    var likelihood = 0.0
    
    val numCols = alpha.numCols()
    val numRows = alpha.numRows()
    
    if (scaled) {
      (0 until numCols).foreach(i => likelihood += Math.exp(alpha.getQuick(numRows - 1, i)))
    
    } else {
      (0 until numCols).foreach(i => likelihood += alpha.getQuick(numRows - 1, i))
      
    }
    
    likelihood
    
  }

  /**
   * Computes the likelihood that a given output sequence was 
   * computed by a given model. Set scaled to true if betas are 
   * log-scaled.
   */
  def modelLikelihood(model:HmmModel,outputSequence:Array[Int],beta:DenseMatrix, scaled:Boolean):Double = {
   
    var likelihood = 0.0

    val B  = model.getBMatrix
    val Pi = model.getPiVector
    
    val numHiddenStates = model.getNumHiddenStates()
    
    val firstOutput = outputSequence(0)
    if (scaled) {
      (0 until numHiddenStates).foreach(i => 
        likelihood += Pi.getQuick(i) * Math.exp(beta.getQuick(0,i)) * B.getQuick(i, firstOutput)
      )
      
    } else {
      (0 until numHiddenStates).foreach(i =>
        likelihood += Pi.getQuick(i) * beta.getQuick(0, i) * B.getQuick(i, firstOutput)
      )
      
    }
    
    likelihood
    
  }

}