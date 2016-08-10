###############################################################################################################
#                                                                                                             #
#                                          How To Use This Script                                             #
#                                                                                                             #
###############################################################################################################

###############################################################################################################
# This script takes in cab trip data from the NYC website and puts it into a SQlite databases. It works with  #
# both yellow and green cab data. Green cab data requires some processing to be able to open it correctly.    #
# Additionally, if data needs to be added to the database, it can also add tables as long as there are no     #
# files in the CSV directory which already have tables in the database. While adding it to the table, this    #
# script will find the neighborhood, borough name and borough code for pickup and dropoff using a shapefile.  #
# These database tables will all have consistent columns, including the neighborhood info.                    #
###############################################################################################################

###############################################################################################################
# Step 1: Download cab trip data you wish to work with from the NYC website                                   #
# (http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) and put all of the CSV files in             #
# one dierctory. They should have names like "green_tripdata_2015-08" or "yellow_tripdata_2015-08".           #
# Ensure you have the files stored on a drive with ample storage, the green cab data files are                #
# duplicated as part of the cleaning process (the duplicates are deleted once after the database is created). #
###############################################################################################################

###############################################################################################################
# Step 2: Make sure you have the "RSQLite" and the "tcltk" libraries installed. You can do so by uncommenting #
# and running the next two lines.                                                                             #
###############################################################################################################

#install.packages("RSQLite")
#install.packages("tcltk")

###############################################################################################################
# Step 3: Change the variables below. "start_dir" should have the filepath of the raw CSV files, while        #
# "output_dir" should be the location where you would like the database to be stored. Again, make sure there  #
# is plenty of space in this directory as it will be a very large file. Lastly, set "database_name" to the    #
# perferred name of the final sqlite file.                                                                    #
###############################################################################################################

#Change these filepaths and database name

start_dir <- "D:/Data/Cab_Data/CSV DUMP/All_Files/dir/"
output_dir<- "D:/Data/"

database_name<- "Final_DB_Cabs1"

###############################################################################################################
#                                                                                                             #
#                                   DO NOT EDIT ANY CODE BELOW THIS LINE!                                     #
#                                                                                                             #
###############################################################################################################



#Importing Libraries & settings
library("RSQLite")
library('tcltk')
library(tigris)
library(dplyr)
library(sp)
library(maptools)
library(broom)
library(httr)
library(rgdal)
options(width=150)

#Get neighborhoods geojson
r <- GET('http://data.beta.nyc//dataset/0ff93d2d-90ba-457c-9f7e-39e47bf2ac5f/resource/35dd04fb-81b3-479b-a074-a27a37888ce7/download/d085e2f8d0b54d4590b1e7d1f35594c1pediacitiesnycneighborhoods.geojson')
nyc_neighborhoods <- readOGR(content(r,'text'), 'OGRGeoJSON', verbose = F)

###############################################################################################################
#                                            Creating Variables                                               #
###############################################################################################################

#Create list of files in the input directory
all_files <- list.files(path = start_dir,pattern = "*.csv")
all_files

# Find the indecies and split the two types of cab data
yellow_indecies<- grep("yellow",all_files,ignore.case=TRUE)
yellow_count<- length(yellow_indecies)

green_indecies<- grep("green",all_files,ignore.case=TRUE) 
green_count<- length(green_indecies)


#Creates list of filepaths for green and yellow
green_filepaths<- paste0(start_dir,all_files[green_indecies])
green_filepaths
sort(green_filepaths)

yellow_filepaths<- paste0(start_dir,all_files[yellow_indecies])
yellow_filepaths
sort(yellow_filepaths)


#create a new list of yellow_tables, which will be the table names for the sqlite db 
# (the filenames, without "tripdata_" or ".csv")
yellow_filenames <- all_files[yellow_indecies]

yellow_tables <- gsub(".csv|tripdata_|",(""),yellow_filenames)
yellow_tables <- gsub("-","_",yellow_tables)
sort(yellow_tables)

#create a new list of green_tables, which will be the table names for the sqlite db (the filenames, without "tripdata_" or ".csv")
green_filenames <- all_files[green_indecies]

green_tables <- gsub(".csv|tripdata_|",(""),green_filenames)
green_tables <- gsub("-","_",green_tables)
sort(green_tables)

###############################################################################################################
#                                        Cleaning Green Cab Files                                             #
###############################################################################################################

#if statement to seperate out the greens, which need to go through cleaning in order to be properly imported
if (green_count >= 1) {
  
  #Create progress bar defaults
  pb <- tkProgressBar(title = "Progress Bar", min = 0, max = length(green_filepaths), width = 500)
  
  for(i in 1:length(green_filepaths)){
    
    setTkProgressBar(pb, i, label=paste('Cleaning Green Cab Data (',i ,'/',length(green_filepaths),') - '
                                        ,round(i/length(green_filepaths)*100, 0),"% Done"))
    
    temp_gree_content<-NULL
    temp_green_content<- try(read.csv(green_filepaths[i], header=TRUE,sep = ",", skip=0),TRUE)
    
    if(class(temp_green_content) =="try-error"){
      
      temp_green_head<- read.csv(green_filepaths[i],header=FALSE,sep = ",", nrows = 1)
      colnames(temp_green_head)<-NULL
      temp_green_content<- read.csv(green_filepaths[i], header=FALSE,sep = ",", skip=3)
      temp_green_content[21:22]<- NULL
      
      
      for (j in 1:length(temp_green_head)) {
        names(temp_green_content)[j]<-toString(temp_green_head[[j]])
      }
    }
    write.csv(temp_green_content,file= gsub('.csv','1.csv',green_filepaths[i]),row.names = FALSE)
  }
}

close(pb)

###############################################################################################################
#                           Writing Both Yellow and Green Data to the Database                                #
###############################################################################################################

#Open Database connection
db_filepath<- paste0(output_dir,database_name,'.sqlite')
db_filepath
db <- dbConnect(SQLite(), dbname= db_filepath)

# Set Progress Bar Settings
pb <- tkProgressBar(title = "Progress Bar", min = 0, max = length(yellow_filepaths), width = 500)

#Creates Yellow Tables
for(l in 1:length(yellow_filepaths)){
  setTkProgressBar(pb, l, label=paste('Writing Yellow Tables to Database (',l,'/',length(yellow_filepaths),') - '
                                      ,round(l/length(yellow_filepaths)*100, 0),"% Done"))
  
  input_file<- read.csv(yellow_filepaths[l],header = TRUE, sep=",")
  
  #Standardize header names and column numbers
  try(names(input_file)[grep("VendorID",names(input_file))]<-"vendor_id")
  try(names(input_file)[grep("vendor_name",names(input_file))]<-"vendor_id")
  try(names(input_file)[grep("lpep_pickup_datetime",names(input_file))]<-"pickup_datetime")
  try(names(input_file)[grep("Lpep_dropoff_datetime",names(input_file))]<-"dropoff_datetime")
  try(names(input_file)[grep("Trip_Pickup_DateTime",names(input_file))]<-"pickup_datetime")
  try(names(input_file)[grep("Trip_Dropoff_DateTime",names(input_file))]<-"dropoff_datetime")
  try(names(input_file)[grep("Passenger_count",names(input_file))]<-"passenger_count")
  try(names(input_file)[grep("Passenger_Count",names(input_file))]<-"passenger_count")
  try(names(input_file)[grep("store_and_forward",names(input_file))]<-"store_and_fwd_flag")
  try(names(input_file)[grep("Store_and_fwd_flag",names(input_file))]<-"store_and_fwd_flag")
  try(names(input_file)[grep("RateCodeID",names(input_file))]<-"rate_code")
  try(names(input_file)[grep("RatecodeID",names(input_file))]<-"rate_code")
  try(names(input_file)[grep("Rate_Code",names(input_file))]<-"rate_code")
  try(names(input_file)[grep("Trip_distance",names(input_file))]<-"trip_distance")
  try(names(input_file)[grep("Trip_Distance",names(input_file))]<-"trip_distance")
  try(names(input_file)[grep("Pickup_longitude",names(input_file))]<-"pickup_longitude")
  try(names(input_file)[grep("Pickup_latitude",names(input_file))]<-"pickup_latitude") 
  try(names(input_file)[grep("Start_Lon",names(input_file))]<-"pickup_longitude") 
  try(names(input_file)[grep("Start_Lat",names(input_file))]<-"pickup_latitude")
  try(names(input_file)[grep("Dropoff_longitude",names(input_file))]<-"dropoff_longitude")
  try(names(input_file)[grep("Dropoff_latitude",names(input_file))]<-"dropoff_latitude") 
  try(names(input_file)[grep("End_Lon",names(input_file))]<-"dropoff_longitude")
  try(names(input_file)[grep("End_Lat",names(input_file))]<-"dropoff_latitude") 
  try(names(input_file)[grep("Fare_amount",names(input_file))]<-"fare_amount") 
  try(names(input_file)[grep("Fare_Amt",names(input_file))]<-"fare_amount")
  try(names(input_file)[grep("Tip_amount",names(input_file))]<-"tip_amount")
  try(names(input_file)[grep("Tip_Amt",names(input_file))]<-"tip_amount")
  try(names(input_file)[grep("Payment_type",names(input_file))]<-"payment_type")
  try(names(input_file)[grep("Payment_Type",names(input_file))]<-"payment_type")
  try(names(input_file)[grep("Total_amount",names(input_file))]<-"total_amount") 
  try(names(input_file)[grep("Total_Amt",names(input_file))]<-"total_amount")
  try(names(input_file)[grep("MTA_tax",names(input_file))]<-"mta_tax")
  try(names(input_file)[grep("Extra",names(input_file))]<-"extra ")
  try(names(input_file)[grep("improvement_surcharge",names(input_file))]<-"surcharge")
  try(names(input_file)[grep("Tolls_amount",names(input_file))]<-"tolls_amount ")
  try(names(input_file)[grep("Tolls_Amt",names(input_file))]<-"tolls_amount")
  try(names(input_file)[grep("tpep_dropoff_datetime",names(input_file))]<-"dropoff_datetime")
  try(names(input_file)[grep("tpep_pickup_datetime",names(input_file))]<-"pickup_datetime")
  try(names(input_file)[grep("tolls_amount ",names(input_file))]<-"tolls_amount")
  
  names(input_file)
  input_file<- input_file[c("pickup_datetime","pickup_longitude","pickup_latitude","dropoff_datetime",
                            "dropoff_longitude","dropoff_latitude","passenger_count","trip_distance",
                            "fare_amount","tip_amount","tolls_amount","total_amount","payment_type",
                            "rate_code","vendor_id","store_and_fwd_flag")]
  
  
  #Filters the data to only import trips that are within the city limits
  input_file<- input_file[which(input_file$trip_distance < 60),]

  input_file<- input_file[which(input_file$pickup_longitude >(-75)),]
  input_file<- input_file[which(input_file$pickup_longitude <(-73)),]

  input_file<- input_file[which(input_file$pickup_latitude >40),]
  input_file<- input_file[which(input_file$pickup_latitude <41.5),]

  input_file<- input_file[which(input_file$dropoff_longitude >(-75)),]
  input_file<- input_file[which(input_file$dropoff_longitude <(-73)),]

  input_file<- input_file[which(input_file$dropoff_latitude >40),]
  input_file<- input_file[which(input_file$dropoff_latitude <41.5),]
  
  
  #Dropoff Neighborhood
  pointsDO<- data.frame(cbind(input_file$dropoff_longitude,input_file$dropoff_latitude))
  names(pointsDO)[1]<-'lng'
  names(pointsDO)[2]<-'lat'
  coordinates(pointsDO) <- ~lng + lat
  proj4string(pointsDO) <- proj4string(nyc_neighborhoods)
  matchesDO <- over(pointsDO, nyc_neighborhoods)
  matchesDO$X.id<-NULL
  matchesDO$borough_pickup<-NULL
  names(matchesDO)<- paste(names(matchesDO),"_dropoff",sep="")
  input_file <- cbind(input_file, matchesDO)
  
  pointsDO<-NULL
  matchesDO<-NULL
  
  #Pickup Neighborhood
  pointsPU<- data.frame(cbind(input_file$pickup_longitude,input_file$pickup_latitude))
  names(pointsPU)[1]<-'lng'
  names(pointsPU)[2]<-'lat'
  coordinates(pointsPU) <- ~lng + lat
  proj4string(pointsPU) <- proj4string(nyc_neighborhoods)
  matchesPU <- over(pointsPU, nyc_neighborhoods)
  matchesPU$X.id<-NULL
  matchesPU$borough_pickup<-NULL
  names(matchesPU)<- paste(names(matchesPU),"_pickup",sep="")
  input_file <- cbind(input_file, matchesPU)
  
  pointsPU<-NULL
  matchesPU<-NULL
  
  
  input_file$neighborhood_pickup
  
  #Write the Table
  db <- dbConnect(SQLite(), dbname= db_filepath)
  dbWriteTable(conn = db, name = yellow_tables[l], value = input_file, overwrite=TRUE)
  
  
}
close(pb)

#Sets progress bar settings
pb <- tkProgressBar(title = "Progress Bar", min = 0, max = length(green_filepaths), width = 500)

green_filepaths
#Creates Green Tables
for(k in 1:length(green_filepaths)){
  
  setTkProgressBar(pb, k, label=paste('Writing Green Tables to Database (',k,'/',length(green_filepaths),') - '
                                      ,round(k/length(green_filepaths)*100, 0),"% Done"))
  
  input_file<- read.csv(gsub(".csv","1.csv",green_filepaths[k]),header = TRUE, sep=",")
  
  #Standardize header names and column numbers
  try(names(input_file)[grep("VendorID",names(input_file))]<-"vendor_id")
  try(names(input_file)[grep("vendor_name",names(input_file))]<-"vendor_id")
  try(names(input_file)[grep("lpep_pickup_datetime",names(input_file))]<-"pickup_datetime")
  try(names(input_file)[grep("Lpep_dropoff_datetime",names(input_file))]<-"dropoff_datetime")
  try(names(input_file)[grep("Trip_Pickup_DateTime",names(input_file))]<-"pickup_datetime")
  try(names(input_file)[grep("Trip_Dropoff_DateTime",names(input_file))]<-"dropoff_datetime")
  try(names(input_file)[grep("Passenger_count",names(input_file))]<-"passenger_count")
  try(names(input_file)[grep("Passenger_Count",names(input_file))]<-"passenger_count")
  try(names(input_file)[grep("store_and_forward",names(input_file))]<-"store_and_fwd_flag")
  try(names(input_file)[grep("Store_and_fwd_flag",names(input_file))]<-"store_and_fwd_flag")
  try(names(input_file)[grep("RateCodeID",names(input_file))]<-"rate_code")
  try(names(input_file)[grep("RatecodeID",names(input_file))]<-"rate_code")
  try(names(input_file)[grep("Rate_Code",names(input_file))]<-"rate_code")
  try(names(input_file)[grep("Trip_distance",names(input_file))]<-"trip_distance")
  try(names(input_file)[grep("Trip_Distance",names(input_file))]<-"trip_distance")
  try(names(input_file)[grep("Pickup_longitude",names(input_file))]<-"pickup_longitude")
  try(names(input_file)[grep("Pickup_latitude",names(input_file))]<-"pickup_latitude") 
  try(names(input_file)[grep("Start_Lon",names(input_file))]<-"pickup_longitude") 
  try(names(input_file)[grep("Start_Lat",names(input_file))]<-"pickup_latitude")
  try(names(input_file)[grep("Dropoff_longitude",names(input_file))]<-"dropoff_longitude")
  try(names(input_file)[grep("Dropoff_latitude",names(input_file))]<-"dropoff_latitude") 
  try(names(input_file)[grep("End_Lon",names(input_file))]<-"dropoff_longitude")
  try(names(input_file)[grep("End_Lat",names(input_file))]<-"dropoff_latitude") 
  try(names(input_file)[grep("Fare_amount",names(input_file))]<-"fare_amount") 
  try(names(input_file)[grep("Fare_Amt",names(input_file))]<-"fare_amount ")
  try(names(input_file)[grep("Tip_amount",names(input_file))]<-"tip_amount")
  try(names(input_file)[grep("Tip_Amt",names(input_file))]<-"tip_amount")
  try(names(input_file)[grep("Payment_type",names(input_file))]<-"payment_type")
  try(names(input_file)[grep("Payment_Type",names(input_file))]<-"payment_type")
  try(names(input_file)[grep("Total_amount",names(input_file))]<-"total_amount") 
  try(names(input_file)[grep("Total_Amt",names(input_file))]<-"total_amount")
  try(names(input_file)[grep("MTA_tax",names(input_file))]<-"mta_tax")
  try(names(input_file)[grep("Extra",names(input_file))]<-"extra ")
  try(names(input_file)[grep("improvement_surcharge",names(input_file))]<-"surcharge")
  try(names(input_file)[grep("Tolls_amount",names(input_file))]<-"tolls_amount ")
  try(names(input_file)[grep("Tolls_Amt",names(input_file))]<-"tolls_amount")
  try(names(input_file)[grep("tpep_dropoff_datetime",names(input_file))]<-"dropoff_datetime")
  try(names(input_file)[grep("tpep_pickup_datetime",names(input_file))]<-"pickup_datetime")
  try(names(input_file)[grep("tolls_amount ",names(input_file))]<-"tolls_amount")
  
  input_file<- input_file[c("pickup_datetime","pickup_longitude","pickup_latitude","dropoff_datetime",
                                 "dropoff_longitude","dropoff_latitude","passenger_count","trip_distance",
                                 "fare_amount","tip_amount","tolls_amount","total_amount","payment_type",
                                 "rate_code","vendor_id","store_and_fwd_flag")]
 
  #Filters the data to only import trips that are within the city limits
  input_file<- input_file[which(input_file$trip_distance < 60),]

  input_file<- input_file[which(input_file$pickup_longitude > (-75)),]
  input_file<- input_file[which(input_file$pickup_longitude <(-73)),]

  input_file<- input_file[which(input_file$pickup_latitude >40),]
  input_file<- input_file[which(input_file$pickup_latitude <41.5),]

  input_file<- input_file[which(input_file$dropoff_longitude >(-75)),]
  input_file<- input_file[which(input_file$dropoff_longitude <(-73)),]

  input_file<- input_file[which(input_file$dropoff_latitude >40),]
  input_file<- input_file[which(input_file$dropoff_latitude <41.5),]
  
  #Dropoff Neighborhood
  pointsDO<- data.frame(cbind(input_file$dropoff_longitude,input_file$dropoff_latitude))
  names(pointsDO)[1]<-'lng'
  names(pointsDO)[2]<-'lat'
  coordinates(pointsDO) <- ~lng + lat
  proj4string(pointsDO) <- proj4string(nyc_neighborhoods)
  matchesDO <- over(pointsDO, nyc_neighborhoods)
  matchesDO$X.id<-NULL
  matchesDO$borough_pickup<-NULL
  names(matchesDO)<- paste(names(matchesDO),"_dropoff",sep="")
  input_file <- cbind(input_file, matchesDO)
  
  pointsDO<-NULL
  matchesDO<-NULL
  
  #Pickup Neighborhood
  pointsPU<- data.frame(cbind(input_file$pickup_longitude,input_file$pickup_latitude))
  names(pointsPU)[1]<-'lng'
  names(pointsPU)[2]<-'lat'
  coordinates(pointsPU) <- ~lng + lat
  proj4string(pointsPU) <- proj4string(nyc_neighborhoods)
  matchesPU <- over(pointsPU, nyc_neighborhoods)
  matchesPU$X.id<-NULL
  matchesPU$borough_pickup<-NULL
  names(matchesPU)<- paste(names(matchesPU),"_pickup",sep="")
  input_file <- cbind(input_file, matchesPU)
  
  pointsPU<-NULL
  matchesPU<-NULL
  
  
  input_file$neighborhood_pickup
  
  #write the table
  db <- dbConnect(SQLite(), dbname= db_filepath)
  dbWriteTable(conn = db, name = green_tables[k], value = input_file, overwrite= TRUE)
  
  
  
}
close(pb)

#Final step removes all of the extra green csv files used in the cleaning process.
file.remove(gsub(".csv","1.csv",green_filepaths))

