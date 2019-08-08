#' dftranslation
#' 
#' dftranslation 
#' @param 
#' dataframe with two columns as validate package required, server and databasename for the supqueries and case values
#' 
#' @return 
#' list of dataframes, first is a dataframe with the rules and labels for each rule. The rest is data frames of each values or output from subquery and case statement.
#' @export
#setMethod(
dftranslation <- function(df,server,databasename){
  # Making variables
  # assembly
  '%&%' <- function(x, y)paste0(x,y)
  # if there is nothing before or after is null there will be nothing
  cleanna <- function(x) {ifelse(is.na(x), "", x)}
  # If there is more than one IS NULL in the sql string
  ecountnull <- function(x){ifelse(str_count(x, "IS NULL")==2,cleanna(word(sapply(strsplit(x, " IS NULL"),"[",2),1,-2)) %&% " is.null(" %&% word(sapply(strsplit(x, " IS NULL"),"[",2),-1) %&% ")=TRUE",cleanna(sapply(strsplit(x, " IS NULL"),"[",2)))}
  # If there is more than one IS NOT NULL in the sql string
  ecountnotnull <- function(x){ifelse(str_count(x, "IS NOT NULL")==2,cleanna(word(sapply(strsplit(x, " IS NOT NULL"),"[",2),1,-2)) %&% " is.null(" %&% word(sapply(strsplit(x, " IS NOT NULL"),"[",2),-1) %&% ")=FALSE",cleanna(sapply(strsplit(x, " IS NOT NULL"),"[",2)))}
  # býr til röð af id fyrir for loopuna
  var <- seq(1, nrow(df))
  
  #dflist[[1]]["new"] <- var
  df["new"] <- var
  dfList <- list()
  #start.time <- Sys.time()
  listnr <-2
  for(val in var)
  {
    # Checking if each line has Select in the string
    #if(grepl("SELECT",dflist[[1]][val,1], perl=TRUE)==TRUE)
    if(grepl("SELECT",df[val,1], perl=TRUE)==TRUE)
    {
      # Count SELECT. If there are two or more SELECT words it get special treatment
      #if(length(grep("SELECT", dflist[[1]][val,1],perl=TRUE))>1)
      #if(str_count(dflist[[1]][val,1],"SELECT")>1)
      if(str_count(df[val,1],"SELECT")>1)
      {
        #mlines <-regmatches(dflist[[1]][val,1], gregexpr("\\(.*?\\)", dflist[[1]][val,1]))[[1]]
        mlines <-regmatches(df[val,1], gregexpr("\\(.*?\\)", df[val,1]))[[1]]
        nlines <- seq(1, length(mlines))
        
        for(n in nlines)
        {
          if(grepl("SELECT",mlines[n],perl = T)==T)
          {
            li <-mlines[n]
            databases <- odbcDriverConnect(paste0("DRIVER={SQL Server}; SERVER=",server,"; trusted_connection=true;database=",databasename))
            d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
            odbcClose(databases)
            if(grepl("[Microsoft][ODBC SQL Server Driver][SQL Server]",d1[[1]][1]))
            {
              databases <- odbcDriverConnect(paste0("DRIVER={SQL Server}; SERVER=",server,"; trusted_connection=true;database=",databasename))
              li<-gsub("^\\(","",li)
              d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
              odbcClose(databases)
              li <- gsub("\\(","\\\\(",li)
              li <- gsub("\\)","\\\\)",li)
            }
            # dfList[[listnr]] <-assign(gsub("(.*?SELECT )|(FROM .*)","",li,perl = TRUE),d1)
            dfList[[listnr]] <-d1
            #dflist[[1]][val,1] <-gsub(li,paste(d1,sep = ","),dflist[[1]][val,1],perl = TRUE)
            df[val,1] <-gsub(li,paste('dfList[[',listnr,']]',sep = ''),df[val,1])
            d1
            rm(d1)
            rm(li)
          }
        }
        rm(mlines)
        rm(nlines)
        listnr <-listnr +1
      }
      # If count is one
      else
      {
        #li <-gsub(".*\\((?=SELECT)|\\)\\sAND+.*|\\)\\sOR+.*|\\)$", "",dflist[[1]][val,1], perl = TRUE)
        li <-gsub(".*\\((?=SELECT)|\\)\\sAND+.*|\\)\\sOR+.*|\\)$", "",df[val,1], perl = TRUE)
        databases <- odbcDriverConnect(paste0("DRIVER={SQL Server}; SERVER=",server,"; trusted_connection=true;database=",databasename))
        d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
        odbcClose(databases)
        if(grepl("[Microsoft][ODBC SQL Server Driver][SQL Server]",d1[[1]][1]))
        {
          #li <-gsub(".*\\((?=SELECT)|\\)$", "",dflist[[1]][val,1], perl = TRUE)
          li <-gsub(".*\\((?=SELECT)|\\)$", "",df[val,1], perl = TRUE)
          databases <- odbcDriverConnect(paste0("DRIVER={SQL Server}; SERVER=",server,"; trusted_connection=true;database=",databasename))
          d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
          odbcClose(databases)
        }
        li <- gsub("\\(","\\\\(",li)
        li <- gsub("\\)","\\\\)",li)
        li <- gsub("\\*","\\\\*",li)
        li <- gsub("\\+","\\\\+",li)
        
        # dfList[[listnr]] <-assign(gsub("(.*?SELECT )|( FROM .*)","",li,perl = TRUE),d1)
        dfList[[listnr]] <-d1
        #dflist[[1]][val,1] <-gsub(li,paste(d1,sep = ","),dflist[[1]][val,1],perl = TRUE)
        df[val,1] <-gsub(li,paste('dfList[[',listnr,']]',sep = ''),df[val,1],perl = TRUE)
        rm(d1)
        rm(li)
        listnr<-listnr+1
      }
    }
  }
  p1 <- 0
  for(val in var)
  {
    if(grepl("CASE",df[val,1], perl=TRUE)==TRUE)
    {
      li <- gsub("^.*(?=CASE)|(?<=END).*","",df[val,1],perl=T)
      databases <- odbcDriverConnect(paste0("DRIVER={SQL Server}; SERVER=",server,"; trusted_connection=true;database=",databasename))
      d1 <- sqlQuery(databases,paste("SELECT",li,sep = " "), stringsAsFactors = FALSE)
      odbcClose(databases)
      li <- gsub("\\(","\\\\(",li)
      li <- gsub("\\)","\\\\)",li)
      dfList[[listnr]] <- d1
      df[val,1]<-gsub(li,paste("dfList[[",listnr,"]]",sep = ""),df[val,1],perl = TRUE)
      rm(li)
      rm(d1)
      
      p1<-p1+1
      listnr<-listnr+1
    }
  }
  rm(var)
  rm(val)
  df <- subset(df, select = -c(new))
  # Change RIGHT function 
  df$rule <- gsub("RIGHT\\((.*?),(.*?)\\)" , "substr(\\1,nchar(\\1)-\\2+1,nchar(\\1))", df$rule)
  # IS NULL
  df$rule[grep(" IS NULL", df$rule, perl = TRUE)] <- gsub("\\s*\\w*$", "", sapply(strsplit(df$rule[grep(" IS NULL", df$rule, perl = TRUE)], " IS NULL"),"[",1)) %&% " is.null(" %&% word(sapply(strsplit(df$rule[grep(" IS NULL", df$rule, perl = TRUE)], " IS NULL"),"[",1),-1) %&% ")=TRUE" %&% ecountnull(df$rule[grep(" IS NULL", df$rule, perl = TRUE)])
  
  # IS NOT NULL
  df$rule[grep(" IS NOT NULL", df$rule, perl = TRUE)] <- gsub("\\s*\\w*$", "", sapply(strsplit(df$rule[grep(" IS NOT NULL", df$rule, perl = TRUE)], " IS NOT NULL"),"[",1)) %&% " is.null(" %&% word(sapply(strsplit(df$rule[grep(" IS NOT NULL", df$rule, perl = TRUE)], " IS NOT NULL"),"[",1),-1) %&% ")=FALSE" %&% ecountnotnull(df$rule[grep(" IS NOT NULL", df$rule, perl = TRUE)])
  # set neitun fyrir framan orð
  # df$rule <- gsub('NOT IN ', '%!in% c', df$rule)
  # splitta "ANDis" fyrir "& is"
  df$rule <- gsub("ANDis", "&& is", df$rule)
  # Skipta út " AND " fyrir " & "
  df$rule <- gsub(" AND ", " && ", df$rule)
  # Skipta út " OR " fyrir " | "
  df$rule <- gsub(" OR ", " | ", df$rule)
  # =
  df$rule <- gsub("=" , "==", df$rule)
  # skipta úr "<==" fyrir "<="
  df$rule <- gsub("<==" , "<=", df$rule)
  # skipta úr "!==" fyrir "!="
  df$rule <- gsub("!==" , "!=", df$rule)
  
  # Skipa út NOT
  df$rule <- gsub('(\\S+) NOT(.*?) &&','!(\\1\\2) &&', df$rule)
  df$rule <- gsub('(\\S+) NOT(.*?$)','!(\\1\\2)', df$rule)
  # skipta út " IN " fyrir " %in% "
  df$rule <- gsub(" IN \\(" , " %in% c(", df$rule)
  # skipta út " LIKE " fyrir " NOT LIKE "
  df$rule <- gsub("(\\w+) NOT LIKE '(\\w+)%'" , "!grepl('\\2',\\1)", df$rule)
  # skipta út " LIKE " fyrir " LIKE "
  df$rule <- gsub("(\\w+) LIKE '(\\w+)%'" , "grepl('\\2',\\1)", df$rule)
  # skipta út " <> " fyrir " != "
  df$rule <- gsub(" <> " , " != ", df$rule)
  df$rule <- gsub("(\\w+)  ([0-9])" , "\\1 = \\2", df$rule)
  df$rule <- gsub("LEN" , "nchar", df$rule)
  dfList[[1]] <- df
  return(dfList)
}