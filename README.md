# Cloud Data Engineering with Apache Spark

## Summary

Apartment rental platforms often disregard (close) public transportation. 
This project connects data of apartment rentals with nearby stations of public transportation.

To this purpose, the project provides routines to move data to AWS S3, conducts ETL operations with Apache Spark, and solves incomplete address data to find surrounding (public) transportation stations using a locally deployed geocoder service. 

Therefore, we integrate various data sources, like apartment rental data (from Kaggle), data of stations within the German public transportation system (from DELFI e.V.), data of a german municipal directory (from Statistisches Bundesamt) with a related mapping (municipality code to zip codes), and the locally deploy 'Photon' geocoding service.


Section Data describes the raw data and the data model. The Setup section leads through all configurations, 
and with section "ETL Operations" you will start the project.



## Data
This project incorporates five data sources. 

1. The Rental dataset shows rental offers in Germany and is available at 
[Kaggle](https://www.kaggle.com/corrieaar/apartment-rental-offers-in-germany). 
Since the file `immo_data.csv` has various columns, we group aspects into three tables: table_rental_price_and_costs,
table_rental_features and tables_rental_location. For brevity, we just show table_rental_location. For the columns of 
the data model see section data model. The table has the following columns (transposed for readability):

  |  _Column_  |  _Example_  |  _Target Table_  |
  -----------|-----------|--------------------
  |  scoutId  |  96107057  |    table_rental_location
  |  geo_bln  |  Nordrhein_Westfalen  |   table_rental_location  
  |  geo_krs  |  Dortmund  |    table_rental_location
  |  geo_plz  |  44269  |    table_rental_location
  |  street  |  Sch&uuml;ruferstra&szlig;e  |    table_rental_location
  |  streetPlain  |  Schüruferstraße  |    table_rental_location
  |  houseNumber  |  244  |    table_rental_location
  |  regio1  |  Nordrhein_Westfalen  |  table_rental_location
  |  regio2  |  Dortmund  |    table_rental_location
  |  regio3  |  Schüren  |    table_rental_location
  |  date  |  May19  |   table_rental_location
  
 <!---
  |  date  |  May19  |   table_rental_location, table_rental_price_and_cost, table_rental_feature  
  |  scoutId  |  96107057  |    table_rental_location, table_rental_price_and_cost, table_rental_feature
  |  serviceCharge  |  245           |  table_rental_price_and_cost
  |  heatingType  |  central_heating  |  table_rental_feature
  |  telekomTvOffer  |  ONE_YEAR_FREE  |  table_rental_feature
  |  telekomHybridUploadSpeed  |  NA  |   table_rental_feature
  |  newlyConst  |  FALSE  |   table_rental_price_and_cost
  |  balcony  |  FALSE  |  table_rental_feature
  |  picturecount  |  6  |   [ not relevant ]
  |  pricetrend  |  4.62  |   table_rental_price_and_cost
  |  telekomUploadSpeed  |  10  |   table_rental_feature
  |  totalRent  |  840  |  table_rental_price_and_cost
  |  yearConstructed  |  1965  |  table_rental_feature
  |  noParkSpaces  |  1  |  table_rental_feature
  |  firingTypes  |  oil  |  table_rental_feature
  |  hasKitchen  |  FALSE  |  table_rental_feature
  |  cellar  |  TRUE  |  table_rental_feature
  |  yearConstructedRange  |  2  |  table_rental_feature
  |  baseRent  |  595  |  table_rental_price_and_cost
  |  livingSpace  |  86   |  table_rental_feature
  |  condition  |  well_kept  |  table_rental_feature
  |  interiorQual  |  normal  |  table_rental_feature
  |  petsAllowed  |  NA  |  table_rental_feature
  |  lift  |  FALSE  |  table_rental_feature
  |  baseRentRange  |  4  |  table_rental_price_and_cost
  |  typeOfFlat  |  ground_floor  |  table_rental_feature
  |  noRooms  |  4  |  table_rental_feature
  |  thermalChar  |  181.4  |  table_rental_feature
  |  floor  |  1  |  table_rental_feature
  |  numberOfFloors  |  3  |  table_rental_feature
  |  noRoomsRange  |  4  |  table_rental_feature
  |  garden  |  TRUE  |  table_rental_feature
  |  livingSpaceRange  |  4  |  table_rental_feature
  |  description  |  "Die ebenerdig zu <...>“ | [ not relevant ]
  |  facilities  |  "Die Wohnung ist <...>“ | [ not relevant ]  
  |  heatingCosts  |  NA  |  table_rental_price_and_cost
  |  energyEfficiencyClass  |  NA  |  table_rental_feature
  |  lastRefurbish  |  NA  |  table_rental_feature
  |  electricityBasePrice  |  NA  |  table_rental_price_and_cost
  |  electricityKwhPrice  |  NA  |  table_rental_price_and_cost

-->



2. The Station dataset provides information on stations of public transportation in Germany 
and is available at [opendata-oepnv.de](https://www.opendata-oepnv.de/ht/de/datensaetze)
(DELFI e.V., "Deutschlandweite Haltestellendaten", 
[Creative Commons Namensnennung CC-BY](http://opendefinition.org/licenses/cc-by/)) 
within the file zHV_aktuell_csv.2021-09-17.csv. 
The table has the following columns (transposed for readability):


|	_Column_	|	_Example_	                    | _Target Table_ | 
|-----------|---------------------------------------|----------------|
|	"SeqNo"	|	"0"	                                | table_majorstations_in_municipals
|	"Type"	|	"S"	                                | table_majorstations_in_municipals
|	"DHID"	|	"de:07334:1714"	                    | table_majorstations_in_municipals
|	"Parent"	|	"de:07334:1714"	                | table_majorstations_in_municipals
|	"Name"	|	"Wörth (Rhein) Alte Bahnmeisterei"	| table_majorstations_in_municipals
|	"Latitude"	|	"49,048672"	                    | table_majorstations_in_municipals
|	"Longitude"	|	"8,266324"	                    | table_majorstations_in_municipals
|	"MunicipalityCode"	|	"07334501"	            | table_majorstations_in_municipals
|	"Municipality"	|	"Wörth am Rhein"	        | table_majorstations_in_municipals
|	"DistrictCode"	|	""	                        | [ not relevant ]
|	"District"	|	""	                            | [ not relevant ]
|	"Condition"	|	"Served"	                    | [ not relevant ]
|	"State"	|	"InOrder"	                        | [ not relevant ]
|	"Description"	|	""	                        | [ not relevant ]
|	"Authority"	|	"NVBW"	                        | [ not relevant ]
|	"DelfiName"	|	"-"	                            | [ not relevant ]
|	"TariffDHID"	|	""	                        | [ not relevant ]
|	"TariffName"	|	""	                        | [ not relevant ]

The columns Latitude and Longitude require the appropriate decimal 
separator. Column DHID refers to column Parent, as both columns describe a hierarchy.



3. Photon is an open-source geocoding, built for OpenStreetMap data (see [Link1](https://github.com/nagyistoce/komoot-photon)
or [Link2](https://github.com/komoot/photon)). It is based on ElasticSearch and enables fast
geocoding of textual address data to coordinates and vice versa.  Section [Setup/Photon](###photon) 
connects a german partition of OpenStreetMap data with Photon.


4. The dataset of the municipality directory of Germany provides the zip code for a given municipality code 
and is provided by Statistisches Bundesamt ("Gemeindeverzeichnis", 30.09.2021 available at 
[destatis](https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/_inhalt.html), 
see "Administrative breakdown" -> "30.09.2021 quarterly issue"). In the S3 bucket, the file `AuszugGV3QAktuell.xlsx` 
contains the sheet "Extract", that provides 
the result of the following transformation:  
    - Map columns "Land", "RB", "Kreis", "Gem" to column "MunicipalityCode"
    - Map column "Gemeindename" to column "Name"
    - Map column "Postleitzahl" to column "PLZ"


The data in the sheet has the following structure (for the reason of readability, the data is transposed):

|Column              | Example          |
|--------------------|------------------|
| MunicipalityCode   | 1001000          | 
| Name               | Flensburg, Stadt |
| PLZ                | 24937            |







<!-- 

5. Dataset refers to (central) coordinates for a given zip code and is available at 
[github.com/WZBSocialScienceCenter](https://github.com/WZBSocialScienceCenter/plz_geocoord/blob/master/plz_geocoord.csv)
   - After copying data from my S3-Bucket, the subdirectory `<your S3 Bucket>/data_collection/` holds the file 
`plz_geocoord.csv`
   - The file has the following structure (for the reason of readability, the data is transposed):

| Column | Example           |
|--------|-------------------|
| PLZ    | 01067             |
| lat    | 51.05754959999999 |
| lng    | 13.7170648        |

-->





### Data model

The main goal is to find nearby (say ten) public transportation stations for a given rental offer.
Therefore, we identify such stations draining all locational information.

A combination of the table_rental_location (this table extracted as a part of the first dataset) and table_stations 
(second dataset) can lead to nearby stations for each apartment rental. 
Unfortunately, both tables do not share identical keys. A solution is to relate both tables 
via zip code and municipal code.

Therefore, we extend table_stations with zip codes and request missing zip codes
at Photon geocoding service. With that, we can locate all stations to rental offers
in table_rental_location via zip codes. For a given apartment and related zip code, we 
query all stations within that zip code and extract the nearest stations via
a k-nearest neighbor approach.

**!!TRANSPOSE all of the following tables!!**

*table_rental_location*

This table holds all locational information of the apartment offers the dataset provides. 
```
+---------+-----+-------+----------+--------+-------+----------+-------+--------------+-----------+-----------+
|  scoutId| date|regio1 |    regio2|  regio3|geo_bln|   geo_krs|geo_plz|        street|streetPlain|houseNumber|
+---------+-----+-------+----------+--------+-------+----------+-------+--------------+-----------+-----------+
|111372798|May19|Sachsen|   Leipzig|Plagwitz|Sachsen|   Leipzig|  04229|Karl - Hein...|Karl_-_H...|          9|
|114400812|Feb20|Sachsen|Mittels...|  Döbeln|Sachsen|Mittels...|  04720|Burgstra&sz...| Burgstraße|         11|
```
The columns represent: 
- geo_bln shows the state
- geo_krs a region within a state 
- geo_plz is a unique zip code
- street, streetPlain and houseNumber represent an address in a region

*table_stations*
```
+-----+----+--------------+--------------+--------------------+---------+---------+----------------+--------------------+
|SeqNo|Type|          DHID|        Parent|                Name| Latitude|Longitude|MunicipalityCode|        Municipality|
+-----+----+--------------+--------------+--------------------+---------+---------+----------------+--------------------+
|  638|   S|de:07334:35017|de:07334:35017|Germersheim Schla...|49.222374| 8.376188|        07334007|         Germersheim|
| 1896|   S| de:08111:2478| de:08111:2478|Stuttgart Zuffenh...|48.823223| 9.185597|        08111000|           Stuttgart|
```
The columns represent:
- DHID is an identifier for a station and refers to "Parent"
- Parent is an identifier for a station
- Name is the name of a station
- Latitude and Longitude are the coordinates for a station
- MunicipalityCode is a unique number in municipality directory of Germany
- Municipality is a (non unique) name  

*table_mapping_municipal_to_zip*
```
+----------------+-----------+-----+
|MunicipalityCode|Name       |  PLZ|
+----------------+-----------+-----+
|01053024        |Düchelsdorf|23847|
|01054079        | Löwenstedt|25864|
```

The columns represent:
- MunicipalityCode is a unique number in municipality directory of Germany
- Municipality is a (non unique) name  
- PLZ is unique zip code

After completing each etl step, the resulting tables will be persisted in S3. The diagramm shows the resulting 
data model:

**!Integrate diagram of data model**


## Setup

### Requirements 
The project requires a *Python 3.6* environment, Pyspark 2.4.3, access to a S3 buckets, and JAVA8-JDK (or higher). 
An AWS EMR instance provides such a setting, and seamlessly connects Spark and S3. 

Next, we describe how to initiate the appropriate AWS S3 and AWS EMR environment. In short, 
- Create an AWS IAM user that has FullS3Access and AdministratorAccess, 
- Create a S3 bucket and set the parameters `INPUT_PATH=s3a://<your AWS S3-URI>/data_collection/`
 and `s3a://<your AWS S3-URI>/data_mart/`  in dl-cfg file,
- Create an AWS EMR instance.
- Afterwards, set up (by collecting photon jar and photon-db) and test Photon geocoding service.

The file dl.cfg collects all relevant information for the data pipeline, what we are about to set up. 
The file dl_example.cfg is a template. Following the next steps, will set the required parameters:  

```
[AWS]
AWS_ACCESS_KEY_ID = <Access KEY without quotes>  
AWS_SECRET_ACCESS_KEY = <Access SECRET without quotes>

[S3]
INPUT_PATH=s3a://<your AWS S3-URI>/data_collection/
OUTPUT_PATH=s3a://<your AWS S3-URI>/data_mart/
```


### AWS IAM user
Create an IAM user at your AWS account by following step 1 to 11 in the 
[AWS instruction](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html#id_users_create_console):  

  - In step 4, select **"Programmatic access"** and **"AWS Management Console access"** with **"Autogenerated password"**  
  - In step 6, select **"Attach existing policies directly"** and separately include the policies **"AmazonS3FullAccess"** and **"AdministratorAccess"** via *type* and *select*.
  - In step 11, download and secretly save the provided **CSV** that contains the credentials. This shows *"User name,
Password,**Access key ID**, **Secret access key**,Console login link"*.

Use the following snippet and insert aws user credentials and temporary save it! 

To access AWS S3 and to process data with AWS EMR, provide the credentials in the file dl.cfg.
Use dl_example.cfg as template.    

```
[AWS]  
AWS_ACCESS_KEY_ID=<**Access key ID** -- without quotes>    
AWS_SECRET_ACCESS_KEY=<**Secret access key** -- without quotes>  
```


To access AWS management console, use the _Console login link_, _User name_, and _Password_.  

### AWS S3 Bucket

1. Generate a random bucket name:
    ```
    import uuid
    import random
    random_key = list(uuid.uuid1().hex)
    random.shuffle(random_key)
    print("".join(random_key[:10]))
    ```

2. Create a new S3 bucket and use the unique the random bucket name. Choose an appropriate regio that is identical for S3 
bucket and AWS EMR instance. I suggest Irland, if you are located in Europe.
You can follow the [AWS instructions](https://docs.aws.amazon.com/quickstarts/latest/s3backup/step-1-create-bucket.html). 

Objekteigentümerschaft -> ACl deaktiviert  
Einstellungen "Öffentlichen Zugriff beschränken" für diesen Bucket -> Blockieren des gesamten öffentlichen Zugriffs  
Bucket-Versioning -> Deaktivieren  
Standardverschlüsselung -> Deaktivieren    


3. In the config file, set the parameters `INPUT_PATH` and `OUTPUT_PATH` equal to
< your AWS S3-URI >. The < your AWS S3-URI > is the root. All etl operations assume, 
the existence of resources in `<your AWS S3-URI>/data_collection/` and 
`<your AWS S3-URI>/data_mart/`.  

```
[S3]
...
INPUT_PATH=s3a://<your AWS S3-URI>/data_collection/
OUTPUT_PATH=s3a://<your AWS S3-URI>/data_mart/
```

(Provided the access privilege and corresponding subfolder ./data_mart/, you could set the OUTPUT_PATH to a local file system.)

Meanwhile, you have an IAM user and the S3 bucket.

4. Get raw data from my S3 Bucket:  
   - The address of the raw data sources is `s3://f3de543c84`
   - Login with your IAM user (see section AWS IAM user using _Console login link_, _User name_, and _Password_)
   and open a CloudShell instance ![img_1.png](img_1.png)  
   - Duplicate the data structure in your S3 bucket using CloudShell:   
      `aws s3 cp s3://f3de543c84 s3://<your AWS S3-URI> --recursive`
<!--
   - Download `immo_data.csv` from [Kaggle](https://www.kaggle.com/corrieaar/apartment-rental-offers-in-germany)
   - Upload the file to `<your AWS S3-URI>/data_collection/immo_data.csv` (E.g. use the
   [AWS instruction](https://docs.aws.amazon.com/quickstarts/latest/s3backup/step-2-upload-file.html))  
-->

Login with your new AWS IAM user using to AWS management console using _Console login link_, _User name_, 
and _Password_.  

Inspecting your S3 bucket shows the subdirectories */data_collection/* and */data_mart/* with all required files 
to access raw data and query saved results within AWS S3.

  

### AWS EMR Cluster with Jupyter notebook
For Amazon EMR version 5.30.0 and later, Python 3 is the system default.

https://towardsdatascience.com/getting-started-with-pyspark-on-amazon-emr-c85154b6b921
(https://towardsdatascience.com/production-data-processing-with-apache-spark-96a58dfd3fe7)

How to start, what config, make availability from public!

https://aws.amazon.com/de/ec2/instance-types/
m4.2xlarge	8 CPU	32GB 	Nur EBS	Bis zu 10	bis zu 4.750

We use Jupyter

<!-- Amazon EMR release versions 5.20.0 and later: Python 3.6 is installed on the cluster instances.) --> 

- EMR Cluster
- Default_Role: +AdministratorAccess
- SecurityRule master: + ssh 22
emr_bootstrap erweitert:
- yum install git
- curl photon.jar
- curl photon-db
AWS EMR JUpyter ist andere instanz als EMR-master!!!
- Github mit Notebook verknüpfen war nicht erfolgreich 
key pair for sh
- vpc mit subnet

### Photo
Photon (https://photon.komoot.io/) as local geocoding service requires the following prerequisites:
   - *After* creation of an AWS EMR instance, open a new terminal within Jupyter  

   - SSH in EMR-master-note
   - `sudo su` 
   - Create and change into new subdirectory    
   - `mkdir ./photon && cd photon`  


   - Checking the Java version should return an AWS Java version  
   `java -version`  


   - Download the Photon release 0.3.5 (https://github.com/komoot/photon/releases/tag/0.3.5) and save it in `./photon/
   `sudo curl -OL photon-0.3.5.jar https://github.com/komoot/photon/releases/download/0.3.5/photon-0.3.5.jar`    
<!--
   - **Completely** deflate the downloaded file into `./photon/` (Pay attention to bz2 and tar compression!)
-->  

   - Download the latest file (around 9 GB)   
   `sudo curl --insecure --output photon-db-latest.tar.bz2 https://download1.graphhopper.com/public/extracts/by-country-code/de/photon-db-de-latest.tar.bz2  | pbzip2 -cd | tar x`  

    `sudo tar -xf photon-db-latest.tar.bz2`


   - As a result, we have the subdirectory (`./photon/photon_data/`. Therein, we only find the sub folder
"elasticsearch", that holds all required data:


   - Start the service  
   `sudo java -jar photon-0.3.5.jar`
<!--   - java -jar photon-0.3.5.jar -listen-port 2330 -->
 
   - Check geocoder service using a test query  
   `curl http://localhost:2322/api/?q=berlin&limit=1&lang=de`

Now your EMR instance is set up to run the project. :-) 

## Project Structure

### Data

Duplicate the data structure from AWS S3 bucket `s3://f3de543c84`. (See section AWS S3 bucket.)

After duplicating from remote S3 bucket, your S3 bucket will have
the subdirectories `/data_collection/` and `/data_mart/` with all corresponding files. 
All raw data is in `/data_collection/`, and ETL operations locate results in `/data_mart/`.  

```
<your AWS S3-URI>/data_collection/AuszugGV3QAktuell.xlsx    
<your AWS S3-URI>/data_collection/plz_geocoord.txt  
<your AWS S3-URI>/data_collection/zHV_aktuell_csv.2021-09-17.csv    
<your AWS S3-URI>/data_collection/immo_dat.csv  
<your AWS S3-URI>/data_mart/  
```

### Code

Cloning the GitHub project in the EMR instance creates following *code* structure to run the project:
  
```
./etl/<*>     
./dl.cfg       
./etl.py      
./README.md       
./requirements.txt  
./emr_bootstrap.sh  
```

In the file `dl.cfg` you need to set the parameters `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, 
`INPUT_PATH`, `OUTPUT_PATH`. (See section AWS IAM user to gather the parameters.)  


**dl_cfg:**

    [AWS]
    SPARK_JARS=com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3,org.apache.hadoop:hadoop-common:2.7.3,com.crealytics:spark-excel_2.11:0.12.2
    S3A_ENDPOINT = s3.amazonaws.com
    AWS_ACCESS_KEY_ID = <Access KEY without quotes>  
    AWS_SECRET_ACCESS_KEY = <Access SECRET without quotes>
    
    [S3]
    SOURCE_PATH=s3://f3de543c84
    INPUT_PATH=<REMOTE AWS S3-URI>
    OUTPUT_PATH=<your AWS S3-URI>
    do_test_S3_access=FALSE
    
    [GENERAL]
    #work_mode = process_inbound,process_outbound
    work_mode = process_outbound

The GitHub projects provides the template file `dl_example.cfg`. 
Rename the file to `dl.cfg` set the four parameters! 


## Execute Pipeline

### Start the Pipeline

   - Start Jupyter notebook by clicking ... or type in terminal `jupyter notebook`.
 
   - Check that project directory contains folders etl and photon, also the files etl_script.ipynb and the dl.cfg file.

   - Open a new terminal and start photon `java -jar photon.3.5.0.jar`

   - Start the Photon service  
   `java -jar photon-0.3.5.jar`
 
   - Check Photon service using a test query  
   `curl http://localhost:2322/api/?q=berlin&limit=1&lang=de`

   - Open etl_script.ipynb and execute all contained sections
    
**Hint**: querying hundreds of thousands of geoinformation from a single Photon instance requires around four hours! 
A future consideration is to pre-build a Photon docker image having the german OpenStreetMap data and 
scale the docker contains using a Nginx as load balancer.      


If updates of the apartment data or the public transportation station occur, rerun the data pipeline. 
As a future consideration, Apache Airflow could regularly update data.
A corresponding Apache Airflow dag contains the upload of dataset to S3 and the execution of the data pipeline.    

## Utilize data model

### Query nearest stations

### Find an appropriate apartment and retrieve the scoutID



## Future Considerations

Later versions of this project can integrate various data sources. Stations of the German Railway and even data
of nearby highways and motorways are desired. Integration of an apartment rental platform via their API makes 
the data integration more flexible. Essentially, the stations need to be structured in groups of zip codes and 
location information from apartments needs to point to such a group of zip codes.

From a technological point of view, if Apache Spark with standalone server mode can not completely process the 
data set, we can distribute data in an AWS EMR cluster for processing large data sets. If 100+ people query 
neighbored stations of different apartments in parallel, we can scale over separate cluster workers or pre-build a 
Photon docker image having the german OpenStreetMap data and scale the docker containers supported by Nginx 
as load balancer.

The underlying querying step finds the corresponding group of zip codes and calculates the smallest distances of 
stations via the k-nearest tree. We can distribute all groups of stations to each worker. Alternatively, we can 
separate groups of stations via partition key using S3 and Parquet.

Apache Airflow is a perfect fit to automatically update the data via the data pipeline and to report data 
quality issues.