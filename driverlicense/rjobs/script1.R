
library(mongolite)

func1 <- function(collection, db, url){

    # Connection to mongodb
    col <- mongo(collection , db, url)

    # documents within collection are transformed into R dataframe
    df <- col$find()

    return(df)
    }

