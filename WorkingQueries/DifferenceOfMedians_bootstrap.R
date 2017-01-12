#' Frequentist difference of medians using bootstrapping

DifferenceOfMedians <- function(x, y, frac.dropped=.2, n.iter=1000, silent=FALSE) {
  #' Given two vectors return the distribution of difference of medians
  #' calculated using bootstrapping
  
  OneDraw <- function() {
    boot.x <- sample(x, round((1-frac.dropped)*length(x)), replace=TRUE)
    boot.y <- sample(y, round((1-frac.dropped)*length(y)), replace=TRUE)
    return(median(boot.x) - median(boot.y))
  }
  draws <- replicate(n.iter, OneDraw())
  if(!silent) {
    cat("Difference of Medians test via the bootstrap\n")
    cat("Median of x:", median(x), "\n")
    cat("Median of y:", median(y), "\n")
    cat("Probability that median(x) > median(y):", round(mean(draws > 0), 2), "\n")
  }
  return(draws)
}

# x1 <- rexp(100, rate=1); x2 <- rexp(90, rate=.89)
# draws <- DifferenceOfMedians(x1, x2, silent=TRUE)
# hist(draws, 50)