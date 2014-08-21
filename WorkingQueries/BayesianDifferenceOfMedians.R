#' Performs a difference of medians test using Bayesian statistics.
#' 
#' It is perfectly resonable to "test until significance" using Bayesian
#' methods when using an informative prior, which we are.  The minimum 
#' meaningful effect is used to set an informative prior for the Bayesian
#' estimation. This keeps us from concluding significance too soon, when
#' we don't have enough data.
#' 
#' Method: Uses MCMCpack::MCMCquantreg to perform quantile regression
#' using a dummy veriable for membership in x (relative to y).

library(MCMCpack)

BayesianDifferenceOfMedians <- function(x, y, minimum.meaningful.effect,
                                        burnin=500,
                                        n.draws=500,
                                        return.draws=FALSE) {
  #' Given two vectors of possibly different lengths
  #' returns the probability that the median of x is greater than
  #' the median of y and draws from the posterior distribution of
  #' median(x) - median(y).  
  #' 
  #' These draws can be used to answer questions like "What is the probability
  #' that the difference of means is between -30 and 30 seconds (ie close to 
  #' zero)?"
  #' 
  #' set the response variable to all the values
  mcmc.y <- c(x, y)
  #' set the dummy variable to caputre 'in group x'
  mcmc.x <- c(rep(1, length(x)), rep(0, length(y)))
  #' regress using quantile regression, which is robust
  posterior  <- MCMCquantreg(mcmc.y ~ mcmc.x, 
                             burnin=burnin, mcmc=n.draws,
                             b0=0, 
                             B0 = matrix(c(1/sd(mcmc.y), 0, 0, 1/minimum.meaningful.effect^2), ncol=2))
  #' get draws for difference of medians
  draws <- posterior[,2]
  #' return probability median(x) > median(y) given the data
  out <- list(prob.x.greater=mean(draws > 0))
  #' return distribution of median(x) - median(y)
  if(return.draws) out <- c(out, list(draws.x.greater=draws))
  return(out)
}
#' x1 <- rnorm(30, mean=100)
#' x2 <- rnorm(35, mean=130)
#' mme <- 30
#' res <- BayesianDifferenceOfMedians(x1, x2, mme, return.draws=TRUE)
#' res$prob.x.greater  # Conclude median(x) < median(y)
#' hist(res$draws.x.greater, 50)
#' mean(res$draws.x.greater < -mme)  # conclude difference is not greater than mme

Probs <- function(draws, mme) {
  #' Given draws from the posterior of median(x) - median(y)
  #' and a minimum meaningful effect (mme) returns the probabilities
  #'   Pr(x wins by more than mme)            labeled 'positive'
  #'   Pr(neither wins by more than mme)      labeled 'near.zero'
  #'   Pr(y wins by more than mme)            labeled 'negative'
  return(c(negative=mean(draws < -mme),
           near.zero=mean(draws < mme & draws > -mme),
           positive=mean(draws > mme)))
}
#' Probs(res$draws.x.greater, mme)  # conclude difference is near zero

SampleSizeNeeded <- function(draws, n.current, mme, certainty) {
  #' Given draws from the posterior of median(x) - median(y),
  #' the total size of the current sample, the minimum meaningful 
  #' effect, and the desired level of certainty, returns an estimate
  #' of the sample size required to reach that level of certainty assuming
  #' that the shape of the posterior remains the same.
  if(median(draws) < -mme) {
    try(out <- (quantile(draws, certainty) - median(draws)) / (-mme - median(draws)) * n.current)
  } else if(median(draws) > mme) {
    try(out <- (median(draws) - quantile(draws, 1 - certainty)) / (median(draws) - mme) * n.current)
  } else {
    try(f <- function(v) mean(draws < v & draws > -v) - certainty)
    try(ur <- uniroot(f, lower=0, upper=max(abs(draws))+1))
    try(out <- (ur$root - median(draws)) / (mme - median(draws)) * n.current)
  }
  out <- round(out)
  names(out) <- NULL
  return(out)
}
#' SampleSizeNeeded(res$draws.x.greater, length(x1)+length(x2), mme, .95)  # We're about at significance
