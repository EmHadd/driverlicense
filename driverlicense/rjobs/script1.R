
# library(mongolite)

func1 <- function(collection, db, url){
    logdebug('I am a silent child')
    # Connection to mongodb
    col <- mongo(collection , db, url)
    loginfo('connected to database')
    # documents within collection are transformed into R dataframe
    df <- col$find()
    loginfo('dataframe name is %s', 'dataframe')

    return(df)
    }

