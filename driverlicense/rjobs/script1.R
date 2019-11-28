
library(mongolite)

func1 <- function(collection, db, url){

    # Connection to mongodb
    col <- mongo(collection , db, url)

    # documents within collection "dwd_report" with "_key" : "report_name" are transformed into R dataframe
    df <- col$find()

    return(df)
    }

