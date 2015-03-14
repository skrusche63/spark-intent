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
 * Implementation of the Forward algorithm
 */
object AlgoForward {
  
  def run(model:HmmModel,observations:Array[Int],scaled:Boolean):DenseMatrix = {
    
    val alpha = new DenseMatrix(observations.length, model.getNumHiddenStates())
    run(model, alpha, observations, scaled)

    alpha
    
  }

  def run(model:HmmModel,alpha:DenseMatrix,observations:Array[Int],scaled:Boolean) {

    val Pi = model.getPiVector
    
    val A = model.getAMatrix
    val B = model.getBMatrix

    val numStates = model.getNumHiddenStates()
    val numObserv = observations.length

    if (scaled) {
      
      /* 
       * Initialization
       */
      (0 until numStates).foreach(i => alpha.setQuick(0, i, Math.log(Pi.getQuick(i) * B.getQuick(i, observations(0)))))

      (1 until numObserv).foreach(t => {        
        (0 until numStates).foreach(i => {
          
          var sum = Double.NegativeInfinity // log(0)
          (0 until numStates).foreach(j => {
            
            val tmp = alpha.getQuick(t - 1, j) + Math.log(A.getQuick(j,i))
            if (tmp > Double.NegativeInfinity) {
              // make sure we handle log(0) correctly
              sum = tmp + Math.log1p(Math.exp(sum - tmp))
            }
          
          })
          
          alpha.setQuick(t, i, sum + Math.log(B.getQuick(i, observations(t))))
        
        })      
      })
      
    } else {

      /* 
       * Initialization
       */
      (0 until numStates).foreach(i => alpha.setQuick(0, i, Pi.getQuick(i) * B.getQuick(i, observations(0))))

      (1 until numObserv).foreach(t => {        
        (0 until numStates).foreach(i => {
          
          var sum = 0.0
          (0 until numStates).foreach(j => sum += alpha.getQuick(t - 1, j) * A.getQuick(j,i))          
          alpha.setQuick(t, i, sum * B.getQuick(i, observations(t)))
        
        })      
      })
    
    }
  
  }

}