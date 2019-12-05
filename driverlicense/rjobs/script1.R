
library(mongolite)
library(logging)

func1 <- function(collection, db, url){
    # Connection to mongodb
    col <- mongo(collection , db, url)
    loginfo('connected to database')
    # documents within collection are transformed into R dataframe
    df <- col$find()
    df$`Kontakte Mio`[df$`Kontakte Mio` == "--"] <- NA
    df$`Kontakte Mio` <- as.numeric(as.character(df$`Kontakte Mio`))
    df$`Kontakte Mio`[is.na(df$`Kontakte Mio`)] <- 0
    # first graph

    firstgraph <- aggregate(df$`Kontakte Mio`, by = list(df$Date), FUN = "sum")
    colnames(firstgraph) <- c("Date", "Kontakte")

    # second graph

    df_new <- df[!is.na(df$Medientyp), ]

    secondgraph <- aggregate(df_new$`Kontakte Mio`, by = list(df_new$Medientyp), FUN = "sum")
    colnames(secondgraph) <- c("Medientyp", "Kontakte")

    # third graph

    thirdgraph <- aggregate(df_new$`Kontakte Mio`, by = list(df_new$Date, df_new$Medientyp), FUN = "sum")
    colnames(thirdgraph) <- c("Date", "Medientyp", "Kontakte")

    loginfo('dataframe name is %s', 'dataframe')
    #ins <- col$insert(df)
    #loginfo('number of inserted records %d', ins$nInserted)
    return(list(firstgraph, secondgraph, thirdgraph))
}

