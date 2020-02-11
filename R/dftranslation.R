#' dftranslation
#' 
#' dftranslation 
#' @param 
#' dataframe with two columns as validate package required. 
#' @param 
#' Server for the subqueries and case values
#' @param 
#' Databasename for the subqueries and case values
#' 
#' @return 
#' list of dataframes, first is a dataframe with the rules and labels for each rule. The rest is data frames of each values or output from subquery and case statement.
#' @export
dfTranslation <- function(df,demo,dbengine,server,databasename,login,password){
  
  # check if stringr is in use
  if("stringr" %in% (.packages())==FALSE)
  {
    stop("This package need Stringr package")
  }
  # if the column names are not right
  if(!("rule" %in% colnames(df)))
  {
    stop("Check the columns name")
  }
  
  # Making a list which is the output
  dfList <- list()
  
  # Making listnumber begin second list
  listnr <-2
  
  # Make sequence for forloop
  var <- seq(1, nrow(df))
  
  # Making new collumn for data frame
  df["new"] <- var
  
  # Making data frame from every subquery
  for(val in var)
  {
    
    # Checking if each line has Select in the string
    if(grepl("SELECT",df[val,1], perl=TRUE)==TRUE)
    {
      # Because this is 
      # check the database engine
      if (dbengine ==2) {
        engine<- 'PostgreSQL'
      } else if (dbengine ==3) {
        engine<- 'MySQL ODBC 5.1 Driver'
      }else{
        engine<- 'SQL Server'
      }
      # check if RODBC is in use
      if("RODBC" %in% (.packages())==FALSE)
      {
        stop("This package need RODBC package")
      }
      # Count SELECT. If there are two or more SELECT words it get special treatment
      if(str_count(df[val,1],"SELECT")>1)
      {
        # Split sentence into miny sentences
        mlines <-regmatches(df[val,1], gregexpr("\\(.*?\\)", df[val,1]))[[1]]
        # Make sequence of mlines
        nlines <- seq(1, length(mlines))
        
        # Go through every line in the sequence
        for(n in nlines)
        {
          # Check if this line has Select inside the line
          if(grepl("SELECT",mlines[n],perl = T)==T)
          {
            # Make a line
            li <-mlines[n]
            # Mae an database connection
            databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; UserName = ",login,", Password =",password," ;database=",databasename))
            # Try to get a data frame
            d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
            # Close 
            odbcClose(databases)
            if(grepl("[Microsoft][ODBC SQL Server Driver][SQL Server]",d1[[1]][1]))
            {
              databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; UserName = ",login,", Password =",password," ;database=",databasename))
              li<-gsub("^\\(","",li)
              d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
              odbcClose(databases)
              li <- gsub("\\(","\\\\(",li,perl = TRUE)
              li <- gsub("\\)","\\\\)",li,perl = TRUE)
            }
            dfList[[listnr]] <-d1
            df[val,1] <-gsub(li,paste('dfList[[',listnr,']]',sep = ''),df[val,1],perl = TRUE)
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
        li <-gsub(".*\\((?=SELECT)|\\)\\sAND+.*|\\)\\sOR+.*|\\)$", "",df[val,1], perl = TRUE)
        databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; UserName = ",login,", Password =",password," ;database=",databasename))
        d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
        odbcClose(databases)
        if(grepl("[Microsoft][ODBC SQL Server Driver][SQL Server]",d1[[1]][1]))
        {
          li <-gsub(".*\\((?=SELECT)|\\)$", "",df[val,1], perl = TRUE)
          databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; UserName = ",login,", Password =",password," ;database=",databasename))
          d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
          odbcClose(databases)
        }
        li <- gsub("\\(","\\\\(",li,perl = TRUE)
        li <- gsub("\\)","\\\\)",li,perl = TRUE)
        li <- gsub("\\*","\\\\*",li,perl = TRUE)
        li <- gsub("\\+","\\\\+",li,perl = TRUE)
        
        dfList[[listnr]] <-d1
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
      # check the database engine
      if (dbengine ==2) {
        engine<- 'PostgreSQL'
      } else if (dbengine ==3) {
        engine<- 'MySQL ODBC 5.1 Driver'
      }else{
        engine<- 'SQL Server'
      }
      # check if RODBC is in use
      if("RODBC" %in% (.packages())==FALSE)
      {
        stop("This package need RODBC package")
      }
      # check if there are demo database
      if (is.null(demo)) { 
        stop("This need  a demo")
      }
      li <- gsub("^.*(?=CASE)|(?<=END).*","",df[val,1],perl=T)
      databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; UserName = ",login,", Password =",password," ;database=",databasename))
      d1 <- sqlQuery(databases,paste("SELECT",li), stringsAsFactors = FALSE)
      odbcClose(databases)
      li <- gsub("\\(","\\\\(",li)
      li <- gsub("\\)","\\\\)",li)
      dfList[[listnr]] <- rep.int(d1,nrow(demo))
      df[val,1]<-gsub(li,paste0("dfList[[",listnr,"]]"),df[val,1],perl = TRUE)
      rm(li)
      rm(d1)
      listnr<-listnr+1
    }
  }
  rm(var)
  rm(val)
  # remove from the data frame
  df <- subset(df, select = -c(new))
  
  # Change RIGHT function which is TRUE
  df$rule <- gsub("RIGHT\\((.*?),(.*?)\\) = \\((.*?)\\)" , "\\(as.integer(substr(\\1,nchar(\\1)-\\2+1,nchar(\\1))) = as.vector\\(\\3\\)\\) = TRUE", df$rule,perl = TRUE)
  df$rule <- gsub("RIGHT\\((.*?),(.*?)\\)=\\((.*?)\\)" , "\\(as.integer(substr(\\1,nchar(\\1)-\\2+1,nchar(\\1))) = as.vector\\(\\3\\)\\) = TRUE", df$rule,perl = TRUE)
  # Change RIGHT function which is FALSE
  df$rule <- gsub("RIGHT\\((.*?),(.*?)\\) != \\((.*?)\\)" , "\\(as.integer(substr(\\1,nchar(\\1)-\\2+1,nchar(\\1))) = as.vector\\(\\3\\)\\) = FALSE", df$rule,perl = TRUE)
  df$rule <- gsub("RIGHT\\((.*?),(.*?)\\)!=\\((.*?)\\)" , "\\(as.integer(substr(\\1,nchar(\\1)-\\2+1,nchar(\\1))) = as.vector\\(\\3\\)\\) = FALSE", df$rule,perl = TRUE)
  # Change Right function with Null
  df$rule <- gsub("RIGHT\\((.*?),(.*?)\\) IS NULL" , "lapply\\(as.integer(substr(\\1,nchar(\\1)-\\2+1,nchar(\\1))),is.na) = TRUE", df$rule,perl = TRUE)
  # Change " <> " for " != " for list
  df$rule <- gsub("([a-zA-Z!@#$%^&*(),.?:{}|<>Þþ1-9_ÐðÁá+-]+) <> \\((.*?)\\)" , "(\\1 = (\\2)) = FALSE", df$rule,perl = TRUE)
  # Change " <> " for " != " for list
  df$rule <- gsub("([a-zA-Z!@#$%^&*(),.?:{}|<>Þþ1-9_ÐðÁá+-]+) <> \\'(.*?)\\'" , "(\\1 = '\\2') = FALSE", df$rule,perl = TRUE)
  # Change " <> " for " != " for list
  df$rule <- gsub("([a-zA-Z!@#$%^&*(),.?:{}|<>Þþ1-9_ÐðÁá+-]+) <> (.*?)" , "\\1 != \\2", df$rule,perl = TRUE)
  
  # IS NULL
  df$rule[grep(" IS NULL", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) IS( ?) NULL','lapply(\\1,is.na) = TRUE',df$rule[grep(" IS NULL", df$rule, perl = TRUE)])
  df$rule[grep(" is null", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) is( ?) null','lapply(\\1,is.na) = TRUE',df$rule[grep(" is null", df$rule, perl = TRUE)])
  
  # IS NOT NULL
  df$rule[grep(" IS NOT NULL", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) IS( ?) NOT( ?) NULL','lapply(\\1,is.na) = FALSE',df$rule[grep(" IS NOT NULL", df$rule, perl = TRUE)])
  df$rule[grep(" is not null", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) is( ?) not( ?) null','lapply(\\1,is.na) = FALSE',df$rule[grep(" is not null", df$rule, perl = TRUE)])
  df$rule[grep(" is Not null", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) is( ?) Not( ?) null','lapply(\\1,is.na) = FALSE',df$rule[grep(" is Not null", df$rule, perl = TRUE)])
  
  # set neitun fyrir framan orð
  # With dfList
  df$rule <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá]+) NOT IN \\(dfList(.*?)\\)', '(\\1 %in% unlist\\(dfList\\2\\)\\)=FALSE', df$rule,perl = TRUE)
  df$rule <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá]+) not in \\(dfList(.*?)\\)', '(\\1 %in% unlist\\(dfList\\2\\)\\)=FALSE', df$rule,perl = TRUE)
  # without dfList
  df$rule <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá]+) NOT IN \\((.*?)\\)', '(\\1 %in% c\\(\\2\\)\\)=FALSE', df$rule,perl = TRUE)
  df$rule <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá]+) not in \\((.*?)\\)', '(\\1 %in% c\\(\\2\\)\\)=FALSE', df$rule,perl = TRUE)
  # skipta út " IN " fyrir " %in% "
  # with dfList
  df$rule <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá]+) IN \\(dfList(.*?)\\)', '(\\1 %in% unlist\\(dfList\\2\\)\\)=TRUE', df$rule,perl = TRUE)
  df$rule <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá]+) in \\(dfList(.*?)\\)', '(\\1 %in% unlist\\(dfList\\2\\)\\)=TRUE', df$rule,perl = TRUE)
  # without dfList
  df$rule <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá]+) IN \\((.*?)\\)', '(\\1 %in% c\\(\\2\\)\\)=TRUE', df$rule,perl = TRUE)
  df$rule <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá]+) in \\((.*?)\\)', '(\\1 %in% c\\(\\2\\)\\)=TRUE', df$rule,perl = TRUE)
  # skipta út "NOT LIKE " fyrir "grepl FALSE"
  df$rule <- gsub('(\\w+) NOT LIKE ([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-90_ÐðÁá+\'-]+)',"grepl(\\2,\\1)=FALSE", df$rule,perl = TRUE)
  df$rule <- gsub('(\\w+) not like ([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-90_ÐðÁá+\'-]+)' , "grepl(\\2,\\1)=FALSE", df$rule,perl = TRUE)
  # Changing " LIKE " for "grepl TRUE"
  df$rule <- gsub('(\\w+) LIKE ([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-90_ÐðÁá+\'-]+)' , "grepl(\\2,\\1)=TRUE", df$rule,perl = TRUE)
  df$rule <- gsub('(\\w+) like ([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-90_ÐðÁá+\'-]+)' , "grepl(\\2,\\1)=TRUE", df$rule,perl = TRUE)
  
  df$rule <- gsub("(\\w+)  ([0-9])" , "\\1 = \\2", df$rule,perl = TRUE)
  # Changing SQL Length to R number of characters
  df$rule <- gsub("LEN\\(" , "nchar\\(", df$rule,perl = TRUE)
  df$rule <- gsub("len\\(" , "nchar\\(", df$rule,perl = TRUE)
  # Changing NOT
  df$rule <- gsub('(\\S+) NOT(.*?) &&','!(\\1\\2) &&', df$rule,perl = TRUE)
  df$rule <- gsub('(\\S+) NOT(.*?$)','!(\\1\\2)', df$rule,perl = TRUE)
  df$rule <- gsub('(\\S+) not(.*?$)','!(\\1\\2)', df$rule,perl = TRUE)
  
  # Changing MONTH to month
  df$rule <- gsub('MONTH','month', df$rule,perl = TRUE)
  # Changing substring
  df$rule <- gsub('SUBSTRING','substr', df$rule,perl = TRUE)
  
  # Changing between
  df$rule <- gsub('(\\w+) BETWEEN (\\w+) & (\\w+)','between(\\1,\\2,\\3)', df$rule,perl = TRUE)
  df$rule <- gsub('(\\w+) between (\\w+) & (\\w+)','between(\\1,\\2,\\3)', df$rule,perl = TRUE)
  df$rule <- gsub('(^.*)','!(\\1)', df$rule,perl = TRUE)
  
  # Changing < or > which has character on right side to number
  df$rule <- gsub('< \'([0-9]+)\'','< \\1', df$rule,perl = TRUE)
  df$rule <- gsub('> \'([0-9]+)\'','> \\1', df$rule,perl = TRUE)
  
  # splitta "ANDis" fyrir "& is"
  df$rule <- gsub("ANDis", "& is", df$rule,perl = TRUE)
  df$rule <- gsub("ANDIS", "& is", df$rule,perl = TRUE)
  df$rule <- gsub("andis", "& is", df$rule,perl = TRUE)
  df$rule <- gsub("andIS", "& is", df$rule,perl = TRUE)
  # Skipta út " AND " fyrir " & "
  df$rule <- gsub(" AND ", " & ", df$rule,perl = TRUE)
  df$rule <- gsub(" and ", " & ", df$rule,perl = TRUE)
  # Skipta út " OR " fyrir " | "
  df$rule <- gsub(" OR ", " | ", df$rule,perl = TRUE)
  df$rule <- gsub(" or ", " | ", df$rule,perl = TRUE)
  # =
  df$rule <- gsub("=" , "==", df$rule,perl = TRUE)
  # skipta úr "<==" fyrir "<="
  df$rule <- gsub("<==" , "<=", df$rule,perl = TRUE)
  # skipta úr "!==" fyrir "!="
  df$rule <- gsub("!==" , "!=", df$rule,perl = TRUE)
  # skipta úr ">==" fyrir ">="
  df$rule <- gsub(">==" , ">=", df$rule,perl = TRUE)
  
  dfList[[1]] <- df
  
  return(dfList)
}