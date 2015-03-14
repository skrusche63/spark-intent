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

import org.apache.mahout.math.{DenseMatrix,DenseVector}

/**
 * Implementation of the Viterbi algorithm to compute the most
 * likely hidden sequence for a given model and observation
 */
object AlgoViterbi {

  def run(model:HmmModel, observations:Array[Int], scaled:Boolean):Array[Int] = {

    val numStates = model.getNumHiddenStates()
    val numObserv = observations.length

    /* 
     * Probability that the most probable hidden states ends 
     * at state i at time t
     */
    val delta = Array.fill[Double](numObserv,numStates)(0.0)

    /* 
     * Previous hidden state in the most probable state leading up 
     * to state i at time t
     */
    val phi = Array.fill[Int](numObserv - 1, numStates)(0)

    val sequence = new Array[Int](numObserv)
    run(sequence, delta, phi, model, observations, scaled)
    
    sequence
  
  }

  def run(sequence:Array[Int], delta:Array[Array[Double]], phi:Array[Array[Int]], model:HmmModel, observations:Array[Int], scaled:Boolean) {
    
    val Pi = model.getPiVector
    
    val A = model.getAMatrix
    val B = model.getBMatrix

    val numStates = model.getNumHiddenStates()
    val numObserv = observations.length
   
    if (scaled) {
      /*
       * Inizialization
       */
      (0 until numStates).foreach(i => delta(0)(i) = Math.log(Pi.getQuick(i) * B.getQuick(i, observations(0))))

      /*
       * Iterate over the time
       */
      (1 until numObserv).foreach(t => {
        /* 
         * Iterate over the hidden states
         */
        (0 until numStates).foreach(i => {
          /* 
           * Find the maximum probability and most likely state
           * leading up to this
           */
          var maxState = 0
          var maxProb = delta(t - 1)(0) + Math.log(A.getQuick(0, i))
          
          (1 until numStates).foreach(j => {
            val prob = delta(t - 1)(j) + Math.log(A.getQuick(j, i))
            if (prob > maxProb) {
              maxProb = prob
              maxState = j
            }
          })
          
          delta(t)(i) = maxProb + Math.log(B.getQuick(i, observations(t)))
          phi(t - 1)(i) = maxState
        
        })
      
      })
    
    } else {
      /*
       * Inizialization
       */
      (0 until numStates).foreach(i => delta(0)(i) = Pi.getQuick(i) * B.getQuick(i, observations(0)))
      
      /*
       * Iterate over the time
       */
      (1 until numObserv).foreach(t => {
        /* 
         * Iterate over the hidden states
         */
        (0 until numStates).foreach(i => {
          /* 
           * Find the maximum probability and most likely state
           * leading up to this
           */
          var maxState = 0
          var maxProb = delta(t - 1)(0) * A.getQuick(0, i)
          
          (1 until numStates).foreach(j => {
            val prob = delta(t - 1)(j) * A.getQuick(j, i)
            if (prob > maxProb) {
              maxProb = prob
              maxState = j
            }
          })
          
          delta(t)(i) = maxProb * B.getQuick(i, observations(t))
          phi(t - 1)(i) = maxState
      
        })
      
      })
      
    }

    /* 
     * Find the most likely end state for initialization
     */
    var maxProb = if (scaled) Double.NegativeInfinity else 0.0
    
    (0 until numStates).foreach(i => {
      if (delta(observations.length - 1)(i) > maxProb) {
        
        maxProb = delta(observations.length - 1)(i)
        sequence(observations.length - 1) = i
      
      }
    
    })

    /* 
     * Backtrack to find the most likely hidden sequence
     */
    for (t <- observations.length - 2 to 0 by -1) {
      sequence(t) = phi(t)(sequence(t + 1))
    }
    
  }

}