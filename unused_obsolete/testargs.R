
args <- as.Date(as.character(commandArgs()),'%y-%m-%d')

print(args)

## print(Sys.time())

changedate <- function(x){
  newdate <- x + 4
  return(newdate)
}

newthing <- changedate(args)
print(newthing)

## write.csv(new_df,file=paste("/Users/ndeprey/Documents/test_",Sys.Date(),".csv",sep=''))
