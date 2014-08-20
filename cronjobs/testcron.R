df <- iris

write.csv(iris,"iris_in_wd.csv")

try(setwd("/home/developer/MPX/cronjobs/results"))

write.csv(iris, "iris_in_results.csv")

getwd()
