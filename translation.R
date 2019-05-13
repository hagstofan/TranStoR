library(RODBC)
library(tidyverse)
library(tibble)
library(stats)
library(proto)
library(gsubfn)
library(proto)
library(RSQLite)
library(sqldf)
library(dplyr)
library(stringr)
library(lubridate)

# Making variables
# assembly
'%&%' <- function(x, y)paste0(x,y)
# if there is nothing before or after is null there will be nothing
cleanna <- function(x) {ifelse(is.na(x), "", x)}
# If there is more than one IS NULL in the sql string
ecountnull <- function(x){ifelse(str_count(x, "IS NULL")==2,cleanna(word(sapply(strsplit(x, " IS NULL"),"[",2),1,-2)) %&% " is.null(" %&% word(sapply(strsplit(x, " IS NULL"),"[",2),-1) %&% ")=TRUE",cleanna(sapply(strsplit(x, " IS NULL"),"[",2)))}
# If there is more than one IS NOT NULL in the sql string
ecountnotnull <- function(x){ifelse(str_count(x, "IS NOT NULL")==2,cleanna(word(sapply(strsplit(x, " IS NOT NULL"),"[",2),1,-2)) %&% " is.null(" %&% word(sapply(strsplit(x, " IS NOT NULL"),"[",2),-1) %&% ")=FALSE",cleanna(sapply(strsplit(x, " IS NOT NULL"),"[",2)))}

# Connecting to the sql database
ch <- odbcDriverConnect("DRIVER={SQL Server}; SERVER=SOME_SERVER; trusted_connection=true")
# get the table
profanir <- sqlQuery(ch,"select skilyrdi, lysing from SOME_TABLE", stringsAsFactors = FALSE)
close(ch)

# býr til röð af id fyrir for loopuna
var <- seq(1, nrow(profanir))

profanir <- mutate(profanir, new = var)


for(val in var)
{
  if(grepl("SELECT",profanir[val,1], perl=TRUE)==TRUE)
  {
    #profanir[val,1] <-"palli"
    databases <- odbcDriverConnect("DRIVER={SQL Server}; SERVER=SOME_SERVER; trusted_connection=true;database=SOME_DATABASE")
    d1 <- sqlQuery(databases,gsub(".*\\((?=SELECT)|\\)\\sAND+.*|\\)\\sOR+.*|\\)$", "",profanir[val,1], perl = TRUE), stringsAsFactors = FALSE)
    odbcClose(databases)
    print(d1[[1]][1])
    if(grepl("[Microsoft][ODBC SQL Server Driver][SQL Server]",d1[[1]][1]))
    {
      profanir[val,1] <-"kalli"
      rm(d1)
    }
    else
    {
      p1 <- as.integer(runif(1, min=0, max=100))
      assign(paste(gsub("(.*?SELECT )|( FROM.*)", "",profanir[val,1], perl = TRUE),p1,sep = ""),d1)
      profanir[val,1] <-gsub("(?<=\\()SELECT.*?(?=\\)\\sAND)|(?<=\\()SELECT.*?(?=\\)$)",paste(gsub("(.*?SELECT )|( FROM.*)", "",profanir[val,1], perl = TRUE),p1,sep = ""),profanir[val,1],perl = TRUE)
      rm(p1)
      rm(d1)
    }
    
  }
}
for(val in var)
{
  if(grepl("CASE",profanir[val,1], perl=TRUE)==TRUE)
  {
    #print(profanir[val,1])
    databases <- odbcDriverConnect("DRIVER={SQL Server}; SERVER=SOME_SERVER; trusted_connection=true;database=SOME_DATABASE")
    profanir[val,1] <-gsub("CASE.*?END",sqlQuery(databases,paste('SELECT',gsub(".*\\((?=CASE)|\\)\\sAND+.*|\\)\\sOR+.*|\\)$", "",profanir[val,1], perl = TRUE)), stringsAsFactors = FALSE),profanir[val,1],perl = TRUE)
    odbcClose(databases)
  }
}
profanir <- subset(profanir, select = -c(new))
# IS NULL
profanir$skilyrdi[grep(" IS NULL", profanir$skilyrdi, perl = TRUE)] <- gsub("\\s*\\w*$", "", sapply(strsplit(profanir$skilyrdi[grep(" IS NULL", profanir$skilyrdi, perl = TRUE)], " IS NULL"),"[",1)) %&% " is.null(" %&% word(sapply(strsplit(profanir$skilyrdi[grep(" IS NULL", profanir$skilyrdi, perl = TRUE)], " IS NULL"),"[",1),-1) %&% ")=TRUE" %&% ecountnull(profanir$skilyrdi[grep(" IS NULL", profanir$skilyrdi, perl = TRUE)])

# IS NOT NULL
profanir$skilyrdi[grep(" IS NOT NULL", profanir$skilyrdi, perl = TRUE)] <- gsub("\\s*\\w*$", "", sapply(strsplit(profanir$skilyrdi[grep(" IS NOT NULL", profanir$skilyrdi, perl = TRUE)], " IS NOT NULL"),"[",1)) %&% " is.null(" %&% word(sapply(strsplit(profanir$skilyrdi[grep(" IS NOT NULL", profanir$skilyrdi, perl = TRUE)], " IS NOT NULL"),"[",1),-1) %&% ")=FALSE" %&% ecountnotnull(profanir$skilyrdi[grep(" IS NOT NULL", profanir$skilyrdi, perl = TRUE)])
# set neitun fyrir framan orð
profanir$skilyrdi <- gsub('(\\w+) NOT', '!\\1', profanir$skilyrdi)
profanir$skilyrdi <- gsub('(\\w+)\\(([^)]*)\\) NOT', '!\\1', profanir$skilyrdi)
# splitta "ANDis" fyrir "& is"
profanir$skilyrdi <- gsub("ANDis", "& is", profanir$skilyrdi)
# Skipta út " AND " fyrir " & "
profanir$skilyrdi <- gsub(" AND ", " & ", profanir$skilyrdi)
# Skipta út " OR " fyrir " | "
profanir$skilyrdi <- gsub(" OR ", " | ", profanir$skilyrdi)
# =
profanir$skilyrdi <- gsub("=" , "==", profanir$skilyrdi)
# skipta úr "<==" fyrir "<="
profanir$skilyrdi <- gsub("<==" , "<=", profanir$skilyrdi)
# skipta úr "!==" fyrir "!="
profanir$skilyrdi <- gsub("!==" , "!=", profanir$skilyrdi)

# skipta út " IN " fyrir " %in% "
profanir$skilyrdi <- gsub(" IN " , " %in% c", profanir$skilyrdi)
# skipta út " LIKE " fyrir " %like% "
profanir$skilyrdi <- gsub("(\\w+) LIKE '(\\w+)%'" , "grepl('^\\2',\\1)", profanir$skilyrdi)
# skipta út " <> " fyrir " != "
profanir$skilyrdi <- gsub(" <> " , " != ", profanir$skilyrdi)
profanir$skilyrdi <- gsub("(\\w+)  ([0-9])" , "\\1 = \\2", profanir$skilyrdi)

profanir$skilyrdi <- gsub("RIGHT\\((.*?),(.*?)\\)" , "substr(\\1,nchar(\\1)-\\2+1,nchar(\\1))", profanir$skilyrdi)
