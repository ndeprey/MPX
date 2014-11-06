library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
library(MCMCpack)
m <- dbDriver("MySQL")
default.platforms <- c('IPHONE', 'ANDROID')

### this only works if you're on the dev server
try(setwd("/home/developer/MPX/cronjobs/results"))

start.date <- Sys.Date() - 4
end.date <- Sys.Date() - 3

##
## Declare Functions ##
##

ABTest <- function(start.date, end.date, test.cohorts, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  # test.cohorts <- c('P','Q')
  
  c1 <- test.cohorts[1]
  c2 <- test.cohorts[2]
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, 
                        (SELECT sum(ratings_elapsed) from user_ratings WHERE ratings_user_id = this.ratings_user_id AND date(ratings_timestamp) >= '",start.date,"' AND date(ratings_timestamp) <= '",end.date,"') as TotalSeconds,
                        (SELECT count(ratings_rating) from user_ratings WHERE ratings_user_id = this.ratings_user_id AND date(ratings_timestamp) >= '",start.date,"' AND date(ratings_timestamp) <= '",end.date,"') as TotalRatings,
                        (SELECT count(ratings_rating) from user_ratings WHERE ratings_cohort NOT IN ('T','U') AND ratings_user_id = this.ratings_user_id AND date(ratings_timestamp) >= '",start.date,"' AND date(ratings_timestamp) <= '",end.date,"') as other,
                        (SELECT count(ratings_rating) from user_ratings WHERE ratings_cohort = '",c1,"' AND ratings_user_id = this.ratings_user_id AND date(ratings_timestamp) >= '",start.date,"' AND date(ratings_timestamp) <= '",end.date,"') as T,
                        (SELECT count(ratings_rating) from user_ratings WHERE ratings_cohort = '",c2,"' AND ratings_user_id = this.ratings_user_id AND date(ratings_timestamp) >= '",start.date,"' AND date(ratings_timestamp) <= '",end.date,"') as U
                        FROM infinite.user_ratings as this
                        WHERE ratings_platform IN ('IPHONE','ANDROID') ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        "GROUP BY ratings_user_id limit 100000000", 
                        sep='')
  rs <- dbSendQuery(con, SQLstatement)
  r <- fetch(rs, n=-1)
  print(paste("the query has", nrow(r), "users"))

  for(i in 1:nrow(r)){
    r$true_cohort[i] <- if(r$T[i] == r$TotalRatings[i]){"T"}
    else if(r$U[i] == r$TotalRatings[i]){"U"}
    else{"other"}
  }
   
  BayesianDifferenceOfMeans <- function(x, y, minimum.meaningful.effect,
                                        burnin=500,
                                        n.draws=500,
                                        return.draws=FALSE) {
    #’ Requires package ‘MCMCpack'
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
  
  CohortEffect <- function(x1,
                           x2,
                           min.meaningful.effect) {
    dat <- data.frame(y=c(x1,x2),
                      d2=c(rep(0, length(x1)), rep(1, length(x2))))
    res <- lm(y ~ d2, data=dat)
    coefs <- summary(res)$coefficients
    
    effect.mean <- coefs[2,1]
    effect.sd <- coefs[2,2]
    
    xmin <- min(c(-min.meaningful.effect, effect.mean - 3*effect.sd))
    xmax <- max(c(min.meaningful.effect, effect.mean + 3*effect.sd))
    ymax <- max(dnorm(0, sd=effect.sd))
    
    prob.near.zero <- pnorm(min.meaningful.effect, mean=effect.mean, sd=effect.sd) -
      pnorm(-min.meaningful.effect, mean=effect.mean, sd=effect.sd)
    prob.positive <- 1 - pnorm(min.meaningful.effect, mean=effect.mean, sd=effect.sd)
    prob.negative <- pnorm(-min.meaningful.effect, mean=effect.mean, sd=effect.sd)
    
    return(list(effect.mean=effect.mean,
                effect.sd=effect.sd,
                min.meaningful.effect=min.meaningful.effect,
                plot.xmin=xmin,
                plot.xmax=xmax,
                plot.ymax=ymax,
                prob.near.zero=prob.near.zero,
                prob.positive=prob.positive,
                prob.negative=prob.negative,
                n=length(x1)+length(x2)))
  }
  
  x1 <- r$TotalSeconds[r$true_cohort== 'T']
  x2 <- r$TotalSeconds[r$true_cohort== 'U']
  
  med1 <- median(r$TotalSeconds[r$true_cohort == 'T'])
  med2 <- median(r$TotalSeconds[r$true_cohort == 'U'])
  mean1 <- mean(r$TotalSeconds[r$true_cohort == 'T'])
  mean2 <- mean(r$TotalSeconds[r$true_cohort == 'U'])
  
  # effect_object <- CohortEffect(r$TotalSeconds[r$true_cohort=='T'], r$TotalSeconds[r$true_cohort=='U'], 30)
  Bayes <- BayesianDifferenceOfMeans(x1,x2, minimum.meaningful.effect = 30, return.draws = TRUE)
  # mean_effect <- effect_object$effect.mean
  
  cohorts_table <- table(r$true_cohort)
  
  return(list(Bayes = Bayes,
              median1 = med1,
              median2 = med2,
              mean1 = mean1,
              mean2 = mean2,
              df=r,
              x1=x1,
              x2=x2,
              cohorts_table = cohorts_table
              ))
}