
print(Sys.time())

df <- data.frame(x=seq(1:10),y=seq(1:10))

write.csv(df,file=paste("/home/developer/cron_results/test",Sys.Date(),".csv",sep=''))
