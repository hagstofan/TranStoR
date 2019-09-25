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
dfTranslation <- function(df,demo,dbengine=1,server,databasename){
    
    # býr til röð af id fyrir for loopuna
    var <- seq(1, nrow(df))
    
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
    
    # check the database engine
    if (dbengine ==2) {
      engine<- 'PostgreSQL'
    } else if (dbengine ==3) {
      engine<- 'MySQL ODBC 5.1 Driver'
    }else{
      engine<- 'SQL Server'
    }
    
    df["new"] <- var
    dfList <- list()
    listnr <-2
    for(val in var)
    {
      
      # Checking if each line has Select in the string
      #if(grepl("SELECT",dflist[[1]][val,1], perl=TRUE)==TRUE)
      if(grepl("SELECT",df[val,1], perl=TRUE)==TRUE)
      {
        # check if RODBC is in use
        if("RODBC" %in% (.packages())==FALSE)
        {
          stop("This package need RODBC package")
        }
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
              databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; trusted_connection=true;database=",databasename))
              d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
              odbcClose(databases)
              if(grepl("[Microsoft][ODBC SQL Server Driver][SQL Server]",d1[[1]][1]))
              {
                databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; trusted_connection=true;database=",databasename))
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
          databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; trusted_connection=true;database=",databasename))
          d1 <- sqlQuery(databases,li, stringsAsFactors = FALSE)
          odbcClose(databases)
          if(grepl("[Microsoft][ODBC SQL Server Driver][SQL Server]",d1[[1]][1]))
          {
            #li <-gsub(".*\\((?=SELECT)|\\)$", "",dflist[[1]][val,1], perl = TRUE)
            li <-gsub(".*\\((?=SELECT)|\\)$", "",df[val,1], perl = TRUE)
            databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; trusted_connection=true;database=",databasename))
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
        databases <- odbcDriverConnect(paste0("DRIVER={",engine,"}; SERVER=",server,"; trusted_connection=true;database=",databasename))
        d1 <- sqlQuery(databases,paste("SELECT",li,sep = " "), stringsAsFactors = FALSE)
        odbcClose(databases)
        li <- gsub("\\(","\\\\(",li)
        li <- gsub("\\)","\\\\)",li)
        dfList[[listnr]] <- rep.int(d1,nrow(demo))
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
    df$rule[grep(" IS NULL", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) IS( ?) NULL','lapply(\\1,is.null) = TRUE',df$rule[grep(" IS NULL", df$rule, perl = TRUE)])
    df$rule[grep(" is null", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) is( ?) null','lapply(\\1,is.null) = TRUE',df$rule[grep(" is null", df$rule, perl = TRUE)])
    
    # IS NOT NULL
    df$rule[grep(" IS NOT NULL", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) IS( ?) NOT( ?) NULL','lapply(\\1,is.null) = FALSE',df$rule[grep(" IS NOT NULL", df$rule, perl = TRUE)])
    df$rule[grep(" is not null", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) is( ?) not( ?) null','lapply(\\1,is.null) = FALSE',df$rule[grep(" is not null", df$rule, perl = TRUE)])
    df$rule[grep(" is Not null", df$rule, perl = TRUE)] <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá+-]+)( ?) is( ?) Not( ?) null','lapply(\\1,is.null) = FALSE',df$rule[grep(" is Not null", df$rule, perl = TRUE)])
    # set neitun fyrir framan orð
    df$rule <- gsub('([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-9_ÐðÁá]+) NOT IN ', '!\\1 %in% c', df$rule)
    # splitta "ANDis" fyrir "& is"
    df$rule <- gsub("ANDis", "& is", df$rule)
    df$rule <- gsub("ANDIS", "& is", df$rule)
    df$rule <- gsub("andis", "& is", df$rule)
    df$rule <- gsub("andIS", "& is", df$rule)
    # Skipta út " AND " fyrir " & "
    df$rule <- gsub(" AND ", " & ", df$rule)
    df$rule <- gsub(" and ", " & ", df$rule)
    # Skipta út " OR " fyrir " | "
    df$rule <- gsub(" OR ", " | ", df$rule)
    df$rule <- gsub(" or ", " | ", df$rule)
    # =
    df$rule <- gsub("=" , "==", df$rule)
    # skipta úr "<==" fyrir "<="
    df$rule <- gsub("<==" , "<=", df$rule)
    # skipta úr "!==" fyrir "!="
    df$rule <- gsub("!==" , "!=", df$rule)
    # skipta úr ">==" fyrir ">="
    df$rule <- gsub(">==" , ">=", df$rule)
    
    
    
    # skipta út " IN " fyrir " %in% "
    df$rule <- gsub(" IN \\(" , " %in% c(", df$rule)
    df$rule <- gsub(" in \\(" , " %in% c(", df$rule)
    # skipta út " LIKE " fyrir " NOT LIKE "
    df$rule <- gsub('(\\w+) NOT LIKE ([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-90_ÐðÁá+\'-]+)',"!grepl(\\2,\\1)", df$rule,perl = TRUE)
    df$rule <- gsub('(\\w+) not like ([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-90_ÐðÁá+\'-]+)' , "!grepl(\\2,\\1)", df$rule,perl = TRUE)
    # skipta út " LIKE " fyrir " LIKE "
    df$rule <- gsub('(\\w+) LIKE ([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-90_ÐðÁá+\'-]+)' , "grepl(\\2,\\1)", df$rule)
    df$rule <- gsub('(\\w+) like ([a-zA-Z!@#$%^&*(),.?":{}|<>Þþ1-90_ÐðÁá+\'-]+)' , "grepl(\\2,\\1)", df$rule)
    # skipta út " <> " fyrir " != "
    df$rule <- gsub(" <> " , " != ", df$rule)
    df$rule <- gsub("(\\w+)  ([0-9])" , "\\1 = \\2", df$rule)
    df$rule <- gsub("LEN\\(" , "nchar\\(", df$rule)
    df$rule <- gsub("len\\(" , "nchar\\(", df$rule)
    # Skipa út NOT
    df$rule <- gsub('(\\S+) NOT(.*?) &&','!(\\1\\2) &&', df$rule,perl = TRUE)
    df$rule <- gsub('(\\S+) NOT(.*?$)','!(\\1\\2)', df$rule,perl = TRUE)
    df$rule <- gsub('(\\S+) not(.*?$)','!(\\1\\2)', df$rule,perl = TRUE)
    
    # Skipta út MONTH
    df$rule <- gsub('MONTH','month', df$rule,perl = TRUE)
    # Skipta út substring
    df$rule <- gsub('SUBSTRING','substr', df$rule,perl = TRUE)
    
    # Skipta út between
    df$rule <- gsub('(\\w+) BETWEEN (\\w+) & (\\w+)','between(\\1,\\2,\\3)', df$rule,perl = TRUE)
    df$rule <- gsub('(\\w+) between (\\w+) & (\\w+)','between(\\1,\\2,\\3)', df$rule,perl = TRUE)
    dfList[[1]] <- df
    return(dfList)
  }