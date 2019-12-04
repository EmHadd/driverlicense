.libPaths()

library(mongolite)

t = {{ df }}
# t
conn <- mongo(collection="{{ config.driverlicense.collection.data.name }}", db="{{ config.driverlicense.collection.data.database }}", url="{{ config.mongo_url }}")
df <- conn$find()
# head(df)

return(df)
return(t)
