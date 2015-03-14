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

import java.util.{Collection,Date,Iterator}

import org.apache.mahout.math.{DenseMatrix,DenseVector}

import scala.util.control.Breaks._

/**
 * three main algorithms are: supervised learning, unsupervised Viterbi and
 * unsupervised Baum-Welch.
 */
object HmmTrainer {

  /**
   * Create an supervised initial estimate of an HMM Model 
   * based on a sequence of observed and hidden states.
   */
  def trainSupervised(numHiddenStates:Int,numOutputStates:Int,observedStates:Array[Int],hiddenStates:Array[Int],pseudoCount:Double):HmmModel = {

    /* Make sure the pseudo count is not zero */
    val pseudo = if (pseudoCount == 0) Double.MinValue else pseudoCount

    val A = new DenseMatrix(numHiddenStates, numHiddenStates)
    val B = new DenseMatrix(numHiddenStates, numOutputStates)
    
    /* 
     * Assign a small initial probability that is larger than zero, 
     * so unseen states will not get a zero probability
     */
    A.assign(pseudo)
    B.assign(pseudo)
    
    /* 
     * Given no prior knowledge, we have to assume that all initial 
     * hidden states are equally likely
     */  
    val Pi = new DenseVector(numHiddenStates)
    Pi.assign(1.0 / numHiddenStates)

    /* Loop over the sequences to count the number of transitions */
    countTransitions(A,B,observedStates,hiddenStates)

    /* Make sure that probabilities are normalized */
    (0 until numHiddenStates).foreach(i => {
      
      /* Compute sum of probabilities for current row of transition matrix */
      var sum = 0.0
      (0 until numHiddenStates).foreach(j => sum += A.getQuick(i,j))
      /* Normalize current row of transition matrix */
      (0 until numHiddenStates).foreach(j => A.setQuick(i,j, A.getQuick(i,j) / sum))
      
      /* Compute sum of probabilities for current row of emission matrix */
      sum = 0.0
      (0 until numOutputStates).foreach(j => sum += B.getQuick(i,j))
      /* Normalize current row of emission matrix */
      (0 until numOutputStates).foreach(j => B.setQuick(i,j, B.getQuick(i,j) / sum))
      
    })

    new HmmModel(A, B, Pi)
    
  }

  /**
   * Function that counts the number of state->state and state->output
   * transitions for the given observed/hidden sequence.
   */
  private def countTransitions(A:DenseMatrix,B:DenseMatrix,observedStates:Array[Int],hiddenStates:Array[Int]) {
    
    B.setQuick(hiddenStates(0), observedStates(0), B.getQuick(hiddenStates(0), observedStates(0)) + 1)
    
    (1 until observedStates.length).foreach(i => {
      
      A.setQuick(hiddenStates(i - 1), hiddenStates(i), A.getQuick(hiddenStates(i - 1), hiddenStates(i)) + 1)
      B.setQuick(hiddenStates(i), observedStates(i),B.getQuick(hiddenStates(i), observedStates(i)) + 1)
    
    })
  
  }

  /**
   * Create an supervised initial estimate of an HMM Model 
   * based on a number of sequences of observed and hidden states.
   */
  def trainSupervisedSequence(numHiddenStates:Int,numOutputStates:Int,observedSeq:Collection[Array[Int]],hiddenSeq:Collection[Array[Int]],pseudoCount:Double):HmmModel = {

    /* Make sure the pseudo count is not zero */
    val pseudo = if (pseudoCount == 0) Double.MinValue else pseudoCount

    val A = new DenseMatrix(numHiddenStates,numHiddenStates)
    val B = new DenseMatrix(numHiddenStates,numOutputStates)
    
    val Pi = new DenseVector(numHiddenStates)

    /* Assign pseudo count to avoid zero probabilities */
    A.assign(pseudo)
    B.assign(pseudo)
    
    Pi.assign(pseudo)

    /* Loop over the sequences to count the number of transitions */
    val hiddenIter = hiddenSeq.iterator()
    val observedIter = observedSeq.iterator()
    
    while (hiddenIter.hasNext() && observedIter.hasNext()) {

      val hiddenStates = hiddenIter.next()
      val observedStates = observedIter.next()
      
      // increase the count for initial probabilities
      Pi.setQuick(hiddenStates(0), Pi.getQuick(hiddenStates(0)) + 1)
      countTransitions(A, B, observedStates, hiddenStates)
    }

    /* Make sure that probabilities are normalized */
    var isum = 0.0 // sum of initial probabilities
    (0 until numHiddenStates).foreach(i => {
      
      isum += Pi.getQuick(i)
      
      /* Compute sum of probabilities for current row of transition matrix */
      var sum = 0.0
      (0 until numHiddenStates).foreach(j => sum += A.getQuick(i,j))
      /* Normalize current row of transition matrix */
      (0 until numHiddenStates).foreach(j => A.setQuick(i,j, A.getQuick(i,j) / sum))
      
      /* Compute sum of probabilities for current row of emission matrix */
      sum = 0
      (0 until numOutputStates).foreach(j => sum += B.getQuick(i,j))
      /* Normalize current row of emission matrix */
      (0 until numOutputStates).foreach(j => B.setQuick(i,j, B.getQuick(i,j) / sum))
    
    })
    
    /* Normalize the initial probabilities */
    (0 until numHiddenStates).foreach(i => Pi.setQuick(i, Pi.getQuick(i) / isum))

    new HmmModel(A, B, Pi)
    
  }

  /**
   * Iteratively train the parameters of the given initial model with regard 
   * to the observed sequence using Viterbi training.
   */
  def trainViterbi(initialModel:HmmModel,observedStates:Array[Int],pseudoCount:Double,epsilon:Double=0.0001,maxIterations:Int=1000,scaled:Boolean=true):HmmModel = {

    /* Make sure the pseudo count is not zero */
    val pseudo = if (pseudoCount == 0) Double.MinValue else pseudoCount

    val lastModel = initialModel.clone()
    val model = initialModel.clone()

    val viterbiPath = new Array[Int](observedStates.length)
    
    val phi = Array.fill[Int](observedStates.length - 1,initialModel.getNumHiddenStates())(0)
    val delta = Array.fill[Double](observedStates.length,initialModel.getNumHiddenStates())(0.0)

    /* Run the Viterbi training iteration */
    breakable {(0 until maxIterations).foreach(i => {
      
      /* Compute the Viterbi path */
      AlgoViterbi.run(viterbiPath, delta, phi, lastModel, observedStates, scaled)
      
      /* 
       * Viterbi iteration uses the viterbi path 
       * to update the probabilities
       */
      val A = model.getAMatrix
      val B = model.getBMatrix

      /* Assign the pseudo count */
      A.assign(pseudo)
      B.assign(pseudo)

      /* Count the transitions */
      countTransitions(A, B, observedStates, viterbiPath)

      val numHiddenStates = model.getNumHiddenStates()
      val numOutputStates = model.getNumOutputStates()
      
      /* Normalize the probabilities */
      (0 until numHiddenStates).foreach(j => {
        
        var sum = 0.0
        /* Normalize the rows of the transition matrix */
        (0 until numHiddenStates).foreach(k => sum += A.getQuick(j,k))
        (0 until numHiddenStates).foreach(k => A.setQuick(j,k, A.getQuick(j,k) / sum))
        
        /* Normalize the rows of the emission matrix */
        sum = 0.0
        (0 until numOutputStates).foreach(k => sum += B.getQuick(j,k))
        (0 until numOutputStates).foreach(k => B.setQuick(j,k, B.getQuick(j,k) / sum))
        
      })
      
      /* Check for convergence */
      if (checkConvergence(lastModel, model, epsilon)) {
        break;
      }
      /* Overwrite the last iterated model by the new iteration */
      lastModel.assign(model);
    
    })}
    
    model
    
  }
  /**
   * Build a randomized initial HMM and use the BaumWelch algorithm to 
   * iteratively train the parameters of the model with regard to the
   * givven observed sequence
   */
  def trainBaumWelch(numHiddenStates:Int,numObservableStates:Int,observations:Array[Int],epsilon:Double=0.0001, maxIterations:Int=1000):HmmModel = {
	      
    /* Construct random-generated HMM */
    val model = new HmmModel(numHiddenStates, numObservableStates, new Date().getTime())

	/* Train model with provided observations */
	trainBaumWelch(model, observations, epsilon, maxIterations, true)
	
  }

  /**
   * Iteratively train the parameters of the given initial model with regard to the
   * observed sequence using Baum-Welch training.
   * 
   * initialModel: The initial model that gets iterated
   * observedSequence: The sequence of observed states
   * 
   * epsilon: Convergence criteria
   * maxIterations: The maximum number of training iterations
   * 
   * scaled: Use log-scaled implementations of forward/backward algorithm. 
   * This is computationally more expensive, but offers better numerical
   * stability for long output sequences.
   * 
   */
  def trainBaumWelch(initialModel:HmmModel,observations:Array[Int],epsilon:Double,maxIterations:Int,scaled:Boolean):HmmModel = {
    
    val model = initialModel.clone()
    val lastModel = initialModel.clone()

    val hiddenCount = model.getNumHiddenStates()
    val visibleCount = observations.length
    
    val alpha = new DenseMatrix(visibleCount, hiddenCount)
    val beta  = new DenseMatrix(visibleCount, hiddenCount)

    /* Run the baum Welch training iteration */
    breakable {for (it <- 0 until maxIterations) {
      
      val Pi = model.getPiVector
      
      val A = model.getAMatrix
      val B = model.getBMatrix      

      val numHiddenStates = model.getNumHiddenStates()
      val numOutputStates = model.getNumOutputStates()
      
      /* Compute forward and backward factors */
      AlgoForward.run(model,alpha,observations,scaled)
      AlgoBackward.run(model,beta,observations,scaled)

      if (scaled) {
        logScaledBaumWelch(observations, model, alpha, beta)
      
      } else {
        unscaledBaumWelch(observations, model, alpha, beta)
      
      }
      
      /* 
       * Normalize transition/emission probabilities
       * and normalize the probabilities
       */
      var isum = 0.0
      for (j <- 0 until numHiddenStates) {

        /* Normalize the rows of the transition matrix */
        var sum = 0.0
        
        (0 until numHiddenStates).foreach(k => sum += A.getQuick(j,k))
        (0 until numHiddenStates).foreach(k => A.setQuick(j,k, A.getQuick(j,k) / sum))
        
        /* Normalize the rows of the emission matrix */
        sum = 0.0
        
        (0 until numOutputStates).foreach(k => sum += B.getQuick(j,k))
        (0 until numOutputStates).foreach(k => B.setQuick(j,k, B.getQuick(j,k) / sum))
        
        /* Normalization parameter for initial probabilities */
        isum += Pi.getQuick(j)
        
      }
      
      /* Normalize initial probabilities */
      (0 until numHiddenStates).foreach(i => Pi.setQuick(i, Pi.getQuick(i) / isum))
      
      /* Check for convergence */
      if (checkConvergence(lastModel, model, epsilon)) {
        break
      }
      
      /* Overwrite the last iterated model by the new iteration */
      lastModel.assign(model)
    
    }}
    
    return model
  
  }

  private def unscaledBaumWelch(observedStates:Array[Int],model:HmmModel,alpha:DenseMatrix,beta:DenseMatrix) {
    
    val Pi = model.getPiVector
    
    val A = model.getAMatrix
    val B = model.getBMatrix
    
    val modelLikelihood = HmmPredictor.modelLikelihood(alpha, false)

    val numHiddenStates = model.getNumHiddenStates()
    val numOutputStates = model.getNumOutputStates()
    
    val numObserv = observedStates.length
 
    for (i <- 0 until numHiddenStates) {
      Pi.setQuick(i, alpha.getQuick(0,i) * beta.getQuick(0,i))
    }

    /* Compute transition probabilities A */
    (0 until numHiddenStates).foreach(i => {
      (0 until numHiddenStates).foreach(j => {
        
        var temp = 0.0
        (0 until numObserv - 1).foreach(t => {
          temp += alpha.getQuick(t,i) * B.getQuick(j, observedStates(t + 1)) * beta.getQuick(t + 1, j)
        })
        
        
        A.setQuick(i, j, A.getQuick(i, j) * temp / modelLikelihood)
      
      })
    })
    
    /* Compute emission probabilities B */
    (0 until numHiddenStates).foreach(i => {
      (0 until numOutputStates).foreach(j => {
        
        var temp = 0.0
        (0 until numObserv).foreach(t => {
          // delta tensor
          if (observedStates(t) == j) {
            temp += alpha.getQuick(t, i) * beta.getQuick(t, i)
          }
        })
        
        B.setQuick(i, j, temp / modelLikelihood)
      
      })
    
    })
  
  }

  private def logScaledBaumWelch(observedSequence:Array[Int],model:HmmModel,alpha:DenseMatrix,beta:DenseMatrix) {
    
    val Pi = model.getPiVector
    
    val A = model.getAMatrix
    val B = model.getBMatrix  
    
    val modelLikelihood = HmmPredictor.modelLikelihood(alpha, true)

    val numHiddenStates = model.getNumHiddenStates()
    val numOutputStates = model.getNumOutputStates()
    
    val sequenceLen = observedSequence.length
    
    (0 until numHiddenStates).foreach(i => Pi.setQuick(i, Math.exp(alpha.getQuick(0,i) + beta.getQuick(0,i))))

    /* Compute transition probabilities */
    (0 until numHiddenStates).foreach(i => {
      (0 until numHiddenStates).foreach(j => {
        
        var sum = Double.NegativeInfinity // log(0)
        (0 until sequenceLen - 1).foreach(t => {
          
          val temp = alpha.getQuick(t, i) + Math.log(B.getQuick(j, observedSequence(t + 1))) + beta.getQuick(t + 1, j)
          if (temp > Double.NegativeInfinity) {
            // handle 0-probabilities
            sum = temp + Math.log1p(Math.exp(sum - temp))
          }
        
        })
        
        A.setQuick(i,j, A.getQuick(i,j) * Math.exp(sum - modelLikelihood))
        
      })
    })
    
    /* Compute emission probabilities */
    (0 until numHiddenStates).foreach(i => {
      (0 until numOutputStates).foreach(j => {

        var sum = Double.NegativeInfinity // log(0)
        
        (0 until sequenceLen).foreach(t => {
          // delta tensor
          if (observedSequence(t) == j) {
            val temp = alpha.getQuick(t, i) + beta.getQuick(t, i)
            if (temp > Double.NegativeInfinity) {
              // handle 0-probabilities
              sum = temp + Math.log1p(Math.exp(sum - temp))
            }
          }
        })
        
        B.setQuick(i, j, Math.exp(sum - modelLikelihood))
      
      })
    
    })
  
  }

  /**
   * Check convergence of two HMM models by computing a simple 
   * distance between emission / transition matrices
   */
  def checkConvergence(oldModel:HmmModel,newModel:HmmModel,epsilon:Double):Boolean = {
    
    /* Convergence of transitionProbabilities */
    val oldA = oldModel.getAMatrix
    val newA = newModel.getAMatrix
    
    val numHiddenStates = oldModel.getNumHiddenStates()
    val numOutputStates = oldModel.getNumOutputStates()
    
    var diff = 0.0
    
    (0 until numHiddenStates).foreach(i => {
      (0 until numHiddenStates).foreach(j => {
        
        val tmp = oldA.getQuick(i,j) - newA.getQuick(i,j)
        diff += tmp * tmp
      
      })
    })
    
    var norm = Math.sqrt(diff)
    
    diff = 0.0
    
    /* Convergence of emissionProbabilities */
    val oldB = oldModel.getBMatrix
    val newB = newModel.getBMatrix  
    
    (0 until numHiddenStates).foreach(i => {
      (0 until numOutputStates).foreach(j => {

        val tmp = oldB.getQuick(i,j) - newB.getQuick(i,j)
        diff += tmp * tmp
        
      })
    })
    
    norm += Math.sqrt(diff)
    (norm < epsilon)
    
  }

}
