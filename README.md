# Cloud Data Engineering With - Apache Spark

## Summary

Since typical apartment rental offers just present apartment related information, the location in connection with good travel connections is a values but often not considered factor.    
This project integrates apartment rental data and surrounding (public) transport hubs data and conducts ETL operations on files from a data lake and processes data with Apache Spark.    

The first version of the project connects historical apartment rental data with data of nearby stations of public transportation.
Using Apache Spark, the first step is to load data to AWS S3 data lake. The second step processes the data resources and writes back tables of extracted data to S3. Results are five tables that model a star schema.

Later versions of the project will integrate data regarding stations of German Railway, data of main nearby highway and motorway notes and APIs of major apartment rental platforms.


## Data
The data sets consists of datasets of files, that are imported in AWS S3 data lakes:

- Apartment rental dataset (available at https://www.kaggle.com/corrieaar/apartment-rental-offers-in-germany) holds the file `immo_data.csv`. It consists of the following columns with an example 

    regio1:  Nordrhein_Westfalen, 
    serviceCharge:  245, 
    heatingType:  central_heating, 
    telekomTvOffer:  ONE_YEAR_FREE, 
    telekomHybridUploadSpeed:  NA, 
    newlyConst:  FALSE, 
    balcony:  FALSE, 
    picturecount:  6, 
    pricetrend:  4.62, 
    telekomUploadSpeed:  10, 
    totalRent:  840, 
    yearConstructed:  1965, 
    scoutId:  96107057, 
    noParkSpaces:  1, 
    firingTypes:  oil, 
    hasKitchen:  FALSE, 
    geo_bln:  Nordrhein_Westfalen, 
    cellar:  TRUE, 
    yearConstructedRange:  2, 
    baseRent:  595, 
    houseNumber:  244, 
    livingSpace:  86, 
    geo_krs:  Dortmund, 
    condition:  well_kept, 
    interiorQual:  normal, 
    petsAllowed:  NA, 
    street:  Sch&uuml;ruferstra&szlig;e, 
    streetPlain:  Schüruferstraße, 
    lift:  FALSE, 
    baseRentRange:  4, 
    typeOfFlat:  ground_floor, 
    geo_plz:  44269, 
    noRooms:  4, 
    thermalChar:  181.4, 
    floor:  1, 
    numberOfFloors:  3, 
    noRoomsRange:  4, 
    garden:  TRUE, 
    livingSpaceRange:  4, 
    regio2:  Dortmund, 
    regio3:  Schüren, 
    description:  Die ebenerdig zu <...>“,
    facilities:  Die Wohnung ist <...>“,
    heatingCosts:  NA, 
    energyEfficiencyClass:  NA, 
    lastRefurbish:  NA, 
    electricityBasePrice:  NA, 
    electricityKwhPrice:  NA, 
    date:  May19

  
- Station dataset (available at `https://www.opendata-oepnv.de/ht/de/datensaetze`, see "Deutschlandweite Haltestellendaten") contains zHV_aktuell_csv.2021-09-17.csv files, like
  
    "SeqNo": "0", 
    "Type": "S", 
    "DHID": "de:07334:1714", 
    "Parent": "de:07334:1714", 
    "Name": "Wörth (Rhein) Alte Bahnmeisterei", 
    "Latitude": "49,048672", 
    "Longitude": "8,266324", 
    "MunicipalityCode": "07334501", 
    "Municipality": "Wörth am Rhein", 
    "DistrictCode": "", 
    "District": "", 
    "Condition": "Served", 
    "State": "InOrder", 
    "Description": "", 
    "Authority": "NVBW", 
    "DelfiName": "-", 
    "TariffDHID": "", 
    "TariffName": ""


- Mapping data munitipality code and zip code (available at `https:/`, see "Deutschlandweite Haltestellendaten") contains AuszugGV3QAktuell.xlsx files, like
    AGS: 01001000, 
    Bezeichnung: Flensburg, Stadt, 
    PLZ: 24937, 


## Data Models

The data from sources is extracted and transformed to

## Prerequisites

The project requires a *Python 3.7* environment, Pyspark 2.4.3., and JAVA8-JDK (or higher). 

To load data from AWS S3, AWS CLI is required, or use the zip files provided in GitHub project. 

To access AWS S3 and to process data with AWS EMR, the config-file dl.cfg in sections [AWS] contains the access information. Use dl_example.cfg as template.
The mandatory arguments are:
  
    [AWS]  
    AWS_ACCESS_KEY_ID = <Access KEY without quotes>  
    AWS_SECRET_ACCESS_KEY = <Access SECRET without quotes>
    region=us-east-1
    output=json


## Getting Started

A running PySpark / Python environment is assumed.  

AWS S3 provides both datasets as a data lake. It is recommended to preload data to a local directory data/ within the Python environment. That speeds up the load process. 

First, connect to the S3 and load data to directory:  

    cd <path to project>
    mkdir data
    cd data

Either extract data/log-data.zip and data/song-data.zip that are provided by this GitHub project in new subfolders (log-data/ or song-data/)

    unzip log-data.zip -d log-data
    unzip song-data.zip

The zip file song-data contains the directory song_data, so rename directory `mkdir song_data, log-data`.

  or copy raw data from AWS S3 in new subfolders (log-data/ or song-data/):

    aws s3 cp s3://udacity-dend/log_data log-data --recursive
    aws s3 cp s3://udacity-dend/song_data song-data --recursive

If files were collected, set the data input and data output path in etl.py, (Project path is equivalent to os.getcwd()):

    input_data = os.path.join(os.getcwd(), "data")  # Alternative: input_data = "s3a://udacity-dend/"
    output_data = os.path.join(os.getcwd(), "data")  # Alternative: output_data = "s3a://<S3 bucket name>/data/"

Second, execute the ETL operations and load the data into the five tables that represent the star schema:  

    python etl.py

Within the ETL step, the resulting tables are persisted in S3. This ETL process could be deployed in an AWS EMR cluster.

VAR OF INTEREST.
regio1 / geo_bln / regio3
serviceCharge
heatingCosts
baseRent
baseRentRange
typeOfFloat
houseNumber
geo_krs
geo_plz
condition
street
streetPlain


## Data Dictionary

  | Variable		            | Description        |
  |-----------------------------|--------------------|
  |  regio1  |  Bundesland  |  
  |  serviceCharge  |  auXilliary costs such as electricty or internet in €  |  
  |  heatingType  |  Type of heating  |  
  |  telekomTvOffer  |  Is payed TV included if so which offer  |  
  |  telekomHybridUploadSpeed  |  how fast is the hybrid inter upload speed  |  
  |  newlyConst  |  is the building newly constructed  |  
  |  balcony  |  does the object have a balcony  |  
  |  picturecount  |    how many pictures were uploaded to the listing  |  
  |  pricetrend  |  price trend as calculated by Immoscout  |  
  |  telekomUploadSpeed  |  how fast is the internet upload speed  |  
  |  totalRent  |  total rent (usually a sum of base rent, service charge and heating cost)  |  
  |  yearConstructed  |  construction year  |  
  |  scoutId  |  immoscout Id  |  
  |  noParkSpaces  |  number of parking spaces  |  
  |  firingTypes  |  main energy sources, separated by colon  |  
  |  hasKitchen  |  has a kitchen  |  
  |  geo_bln  |  bundesland (state), same as regio1  |  
  |  cellar  |  has a cellar  |  
  |  yearConstructedRange  |  binned construction year, 1 to 9  |  
  |  baseRent  |  base rent without electricity and heating  |  
  |  houseNumber  |  house number  |  
  |  livingSpace  |  living space in sqm  |  
  |  geo_krs  |  district, above ZIP code  |  
  |  condition  |  condition of the flat  |  
  |  interiorQual  |  interior quality  |  
  |  petsAllowed  |  are pets allowed, can be yes, no or negotiable  |  
  |  street  |  street name  |  
  |  streetPlain  |  street name (plain, different formating)  |  
  |  lift  |  is elevator available  |  
  |  baseRentRange  |  binned base rent, 1 to 9  |  
  |  typeOfFlat  |  type of flat  |  
  |  geo_plz  |  ZIP code  |  
  |  noRooms  |  number of rooms  |  
  |  thermalChar  |  energy need in kWh/(m^2a), defines the energy efficiency class  |  
  |  floor  |  which floor is the flat on  |  
  |  numberOfFloors  |  number of floors in the building  |  
  |  noRoomsRange  |  binned number of rooms, 1 to 5  |  
  |  garden  |  has a garden  |  
  |  livingSpaceRange  |  binned living space, 1 to 7  |  
  |  regio2  |  District or Kreis, same as geo krs  |  
  |  regio3  |  City/town  |  
  |  description  |  free text description of the object  |  
  |  facilities  |  free text description about available facilities  |  
  |  heatingCosts  |  monthly heating costs in €  |  
  |  energyEfficiencyClass  |  energy efficiency class (based on binned thermalChar, deprecated since Feb 2020)  |  
  |  lastRefurbish  |  year of last renovation  |  
  |  electricityBasePrice  |  monthly base price for electricity in € (deprecated since Feb 2020)  |  
  |  electricityKwhPrice  |  electricity price per kwh (deprecated since Feb 2020)  |  
  |  date  |  time of scraping  |  