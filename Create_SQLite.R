###############################################################################################################
#                                                                                                             #
#                                          How To Use This Script                                             #
#                                                                                                             #
###############################################################################################################

###############################################################################################################
# This script takes in cab trip data from the NYC website and puts it into a SQlite databases. It works with  #
# both yellow and green cab data. Green cab data requires some processing to be able to open it correctly.    #
# Additionally, if data needs to be added to the database, it can also add tables as long as there are no     #
# files in the CSV directory which already have tables in the database.                                       #
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

start_dir <- "D:/Data/Cab_Data/CSV DUMP/All_Files/"
output_dir<- "D:/Data/Cab_Data/"

database_name<- "Final_test6"

###############################################################################################################
#                                                                                                             #
#                                   DO NOT EDIT ANY CODE BELOW THIS LINE!                                     #
#                                                                                                             #
###############################################################################################################



#Importing Libraries & settings
library("RSQLite")
library('tcltk')
options(width=150)

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
    temp_green_content<- try(read.csv(green_filepaths[i], header=TRUE,sep = ",", skip=0, nrows = 5),TRUE)
    
  if(class(temp_green_content) =="try-error"){
    
      temp_green_head<- read.csv(green_filepaths[i],header=FALSE,sep = ",", nrows = 1)
      colnames(temp_green_head)<-NULL
      temp_green_content<- read.csv(green_filepaths[i], header=FALSE,sep = ",", skip=3, nrows = 5)
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
  input_file<- read.csv(yellow_filepaths[l],header = TRUE, sep=",", nrows = 6)
  
  dbWriteTable(conn = db, name = yellow_tables[l], value = input_file)
  
  setTkProgressBar(pb, l, label=paste('Writing Yellow Tables to Database (',l,'/',length(yellow_filepaths),') - '
                                                            ,round(l/length(yellow_filepaths)*100, 0),"% Done"))
  
  
}
close(pb)

#Sets progress bar settings
pb <- tkProgressBar(title = "Progress Bar", min = 0, max = length(green_filepaths), width = 500)

#Creates Green Tables
for(k in 1:length(green_filepaths)){
  input_file<- read.csv(gsub(".csv","1.csv",green_filepaths[k]),header = TRUE, sep=",")
  
  dbWriteTable(conn = db, name = green_tables[k], value = input_file)
  
  setTkProgressBar(pb, l, label=paste('Writing Green Tables to Database (',k,'/',length(green_filepaths),') - '
                                                            ,round(k/length(green_filepaths)*100, 0),"% Done"))
  
}
close(pb)

#Final step removes all of the extra green csv files used in the cleaning process.
file.remove(gsub(".csv","1.csv",green_filepaths))

