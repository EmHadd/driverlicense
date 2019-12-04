
# library(mongolite)

func1 <- function(collection, db, url){
    # Connection to mongodb
    col <- mongo(collection , db, url)
    loginfo('connected to database')
    # documents within collection are transformed into R dataframe
    df <- col$find()
    loginfo('dataframe name is %s', 'dataframe')
    ins <- col$insert(df)
    loginfo('number of inserted records %d', ins$nInserted)
    return(df)
    }

