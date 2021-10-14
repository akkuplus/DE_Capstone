import os
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import StructType, StructField, StringType

import helper

""" At first, unrelevant!
print("\n\ntable_rental_feature:")
location = os.path.join(input_path, "table_rental_feature.parquet")
table_rental_feature = spark.read.parquet(location)
table_rental_feature.show()
"""

print("\n\ntable_rental_location")
location = os.path.join(input_path, "table_rental_location.parquet")
table_rental_location = spark.read.parquet(location)
table_rental_location.show()

"""
TODOs:

"""

print(f"Anzahl aller Werte: {table_rental_location.count()}")
print(f"Anzahl gleicher Werte für Bundesland: {table_rental_location.agg(F.count(F.when(F.col('regio1') == F.col('geo_bln'), 1))).show()}")
print(f"Anzahl gleicher Werte für Kreise: {table_rental_location.agg(F.count(F.when(F.col('regio2') == F.col('geo_krs'), 1))).show()}")
# -> Spalten zu Bundesländern und kreise enthalten nicht identische Werte -> kein eindeutiges Matching möglich

relev_cols = table_rental_location.columns[2:-3]
hmm = table_rental_location.select(relev_cols).collect().show()
print(f"Anzahl auswertbarer Werte für Postleitzahl: {table_rental_location.agg(F.count(F.when(F.col('geo_PLZ'), 1))).show()}")




print("\n\ntable_stations")
location = os.path.join(input_path, "table_stations.parquet")
table_stations = spark.read.parquet(location)
table_stations.show()

print("\n\ntable_stations_in_municipals")
location = os.path.join(input_path, "table_stations_in_municipals.parquet")
table_stations_in_municipals = spark.read.parquet(location)
table_stations_in_municipals.show()

print("\n\ntable_Mapping_municipal_code_to_zip")
location = os.path.join(input_path, "table_Mapping_municipal_code_to_zip.parquet")
table_Mappig_municipal_code_to_zip = spark.read.parquet(location)
table_Mappig_municipal_code_to_zip.show()


temp1.unique()

['01059014', '01059016', '01059021', '01059064', '03152001',
      '03152011', '03152012', '03152016', '03152021', '03152026',
      '03358004', '03358022', '06633021', '06633027', '07132074',
      '07132084', '07339027', '07339061', '12073505', '14521030',
      '14521050', '14625540', '14729230', '14729290', '16056000',
      '16061052', '16062004', '16062016', '16062018', '16062036',
      '16062059', '16063013', '16063019', '16063024', '16063029',
      '16063052', '16063055', '16063075', '16063081', '16063089',
      '16063094', '16063102', '16064008', '16064029', '16064035',
      '16064048', '16064052', '16064057', '16065023', '16065061',
      '16065072', '16065082', '16065084', '16066067', '16066071',
      '16067025', '16067039', '16067054', '16067083', '16067085',
      '16067086', '16068026', '16068047', '16069039', '16069048',
      '16069059', '16070003', '16070018', '16070032', '16070042',
      '16070044', '16070055', '16070056', '16071006', '16071012',
      '16071034', '16071036', '16071057', '16071065', '16071067',
      '16071073', '16071088', '16071099', '16072001', '16072005',
      '16072009', '16072014', '16073036', '16073101', '16073108',
      '16075009', '16075018', '16075049', '16075061', '16076052',
      '16077002', '16077004', '16077006', '16077019', '16077029',
      '16077037', '16077051', '16077055']

temp2 = dfpd_stations_with_locations.loc[dfpd_stations_with_locations["MunicipalityCode"].isna(), "AGS"]
temp2.unique()
['01053105', '01054039', '01056025', '01058055', '01058089',
       '01061039', '01061053', '01061057', '01061068', '03151501',
       '03153504', '03154501', '03154502', '03154503', '03154504',
       '03154506', '03158501', '03158502', '03158503', '03255501',
       '03255502', '03255503', '03255505', '03255508', '03257036',
       '03358024', '03452501', '03454060', '03457501', '06431200',
       '06435200', '06633030', '06636200', '07000999', '07131033',
       '07132060', '07132502', '07137218', '07138042', '07143271',
       '07143315', '07231020', '07231021', '07232066', '08317971',
       '08336080', '08336089', '08415971', '09172452', '09172454',
       '09173451', '09173452', '09175451', '09175452', '09175453',
       '09176451', '09180451', '09181451', '09183451', '09184452',
       '09184454', '09184457', '09187451', '09187452', '09188451',
       '09189451', '09189452', '09272451', '09272452', '09272453',
       '09272455', '09272456', '09272457', '09272458', '09272459',
       '09272460', '09272461', '09272463', '09273451', '09273452',
       '09273453', '09273454', '09371452', '09374451', '09374452',
       '09374458', '09375451', '09375452', '09376455', '09471452',
       '09471453', '09471454', '09471455', '09471456', '09471457',
       '09471459', '09471460', '09471461', '09471462', '09472451',
       '09472453', '09472454', '09472456', '09472458', '09472463',
       '09472464', '09472468', '09472469', '09472470', '09473452',
       '09473453', '09473454', '09475451', '09475452', '09475453',
       '09475454', '09476451', '09476453', '09478451', '09478453',
       '09479453', '09479455', '09479456', '09479457', '09479459',
       '09479460', '09479461', '09479462', '09479463', '09571451',
       '09572451', '09572452', '09572453', '09572454', '09572455',
       '09572456', '09572457', '09572458', '09572459', '09572460',
       '09574451', '09574452', '09574453', '09574454', '09574455',
       '09574456', '09574457', '09574458', '09574460', '09574461',
       '09574462', '09574463', '09574464', '09574465', '09575451',
       '09576451', '09576452', '09576453', '09576454', '09576455',
       '09671451', '09671453', '09671456', '09671457', '09671458',
       '09671459', '09671460', '09671461', '09672451', '09672454',
       '09672455', '09672456', '09672457', '09672458', '09672461',
       '09672462', '09672463', '09672464', '09672465', '09672466',
       '09672468', '09673451', '09673452', '09673453', '09673454',
       '09673455', '09673456', '09673457', '09673458', '09676452',
       '09676455', '09677452', '09677453', '09677454', '09677455',
       '09677456', '09677457', '09677458', '09677459', '09677461',
       '09677463', '09678451', '09678452', '09678453', '09678454',
       '09678455', '09678456', '09678457', '09679451', '09679452',
       '09679453', '09772451', '09774451', '09774452', '09775451',
       '09775452', '09775454', '09775455', '09778451', '09779452',
       '09779453', '09780451', '10042999', '13000999', '16063104',
       '16063105', '16064077', '16065089', '16067089', '16067092',
       '16071103', '16072024', '16076038']

return

# Processing
#input_path = os.path.join(os.getcwd(), "data")
input_path = "s3a://aws-emr-resources-726459035533-us-east-1/data/"

# STATIONS data
name = "table_stations_in_municipals"
data_location = os.path.join(input_path, f"{name}.parquet")
assert os.path.exists(data_location)
table_stations_in_municipals = spark.read.parquet(data_location)
table_stations_in_municipals.show(15, truncate=True)

# Mappping from municipality to zip
name = "table_Mapping_municipal_code_to_zip"
data_location = os.path.join(input_path, f"{name}.parquet")
assert os.path.exists(data_location)
table_mapping_municipal_ZIP = spark.read.parquet(data_location)
table_mapping_municipal_ZIP.show(15, truncate=True)

cond = [table_stations_in_municipals.MunicipalityCode == table_mapping_municipal_ZIP.AGS]
table_stations_with_zip = table_stations_in_municipals.join(table_mapping_municipal_ZIP, cond, "left")
table_stations_with_zip.show(50, truncate=True)

if False:
    # any missing values for zip joint code?
    any(table_stations_with_zip["PLZ"].isna()) # yes around 1000 rows have no zip code -> handle such cases

    table_stations_with_zip.loc[table_stations_with_zip["PLZ"].isna(), ["PLZ"]] # shows all rows that have a missing zip code

# Idee: führe die Informationen aus table_stations_with_zip mit table_rental_location zusammen.
# table_stations_with_zip kann je Postleitzahl viele Haupt / Parent Station haben.
# Nimm diese mit kürzester Distanz anhand von Lat Long und extrahiere hierfür die Parent-ID der Station.
# Wenn Parent-Station gefunden / vorhanden, joint alle Stationen heran, die zur Parent-Station gehören.

##### Mapping of municipality codes to zip codes

from pyspark.sql.functions import udf
import org.apache.spark.sql.expressions.UserDefinedFunction

def convertCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
        resStr = resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

convertUDF = udf(lambda z: convertCase(z), StringType())

df.select(col("Seqno"), convertUDF(col("Name")).alias("Name")).show(truncate=False)

@udf("float")
def find_zip_udf(lat, long):
    s = 1
    return s * s

df = spark.table("test")
display(df.select("id", squared_udf("id").alias("id_squared")))


s1 = set(table_Mapping_Municipal_ZIP.select(["AGS"]).collect())
s2 = set(table_Mapping_Municipal_ZIP.select(["MunicipalityCode"]).collect())

len(s1)                             # len = 11000
len(s2)                             # len = 10870
len(s2.intersection(s1))            # 10767
len(s1.intersection(s2))            # 10767
len(s1.difference(s2))              # 233
len(s2.difference(s1))              # 103
len(s1.symmetric_difference(s2))    # 336
len(s2.symmetric_difference(s1))    # 336

#


# RENTAL data
temp_input_path = os.path.join(os.getcwd(), "data", "_old")
data_location = os.path.join(temp_input_path, "table_rental_location.parquet")
assert os.path.exists(data_location)
table_rental_location = spark.read.parquet(data_location)
table_rental_location.show(15, truncate=True)

table_rental_location_kt = table_rental_location.select(["scoutId","date","geo_plz"])
table_stations_with_zip_kt = table_stations_with_zip.select(["PLZ", "Parent"])
cond = [table_rental_location_kt.geo_plz == table_stations_with_zip_kt.PLZ]

table_rentals_with_stations = table_rental_location_kt.join(table_stations_with_zip_kt, cond, "left")
table_rentals_with_stations.show(50, truncate=True)

output_path = os.path.join(os.getcwd(), "data")
output_location = os.path.join(output_path, "table_rentals_with_stations.parquet")
table_rentals_with_stations.write.mode('overwrite').parquet(output_location)
helper.logger.debug("Exported table_rentals_with_stations as parquet")


# Zuspielen der Nebenstationen: Strategien
# 1. Strategie: Aus table_rentals_with_stations suche anhand des Wertes von "Parent" zugeordnete Haltestellen mit gleichem Wert für Parent
# 2. Strategie: In table_rentals_with_stations suche Zeilen ohne (Parent, Lat und Long)
# -> Spiele anhand der geo_PLZ die (Lat, Lang) für den Wert der geo_PLZ an -> Anhand von Werten (lat, Long) suche nächste Haltestellen!
#

# 1. Strategie:
data_location = r"C:\Users\marcus\Documents\_Working\DatEng_Capstone\data\table_stations.parquet"
os.path.exists(data_location)
table_stations = spark.read.parquet(data_location)
table_stations.show()

table_rentals_with_stations_having_Parent = table_rentals_with_stations\
    .withColumnRenamed('Parent', 'Parent_ID')\
    .filter(table_rentals_with_stations.Parent.isNotNull())
table_rentals_with_stations_having_Parent.show()

cond = table_rentals_with_stations_having_Parent.Parent_ID == table_stations.Parent
table_stations_kt = table_stations.select(["Parent", "DHID", "SeqNo", "Name", "Latitude", "Longitude", "Municipality", "Authority"])
table_rentals_all_stations_having_Parent = table_rentals_with_stations_having_Parent.join(table_stations_kt, cond)
table_rentals_all_stations_having_Parent.show(50, truncate=False)

output_path = os.path.join(os.getcwd(), "data")
output_location = os.path.join(output_path, "table_rentals_all_stations_having_Parent.parquet")
table_rentals_all_stations_having_Parent.write.mode('overwrite').parquet(output_location)
helper.logger.debug("Exported table_rentals_all_stations_having_Parent as parquet")

# 2. Strategie:
table_rentals_with_stations_having_NO_Parent = table_rentals_with_stations.filter(table_rentals_with_stations.Parent.isNull())
table_rentals_with_stations_having_NO_Parent.show()

# IMPORT
# der csv-Daten: table_mapping_zip_2_coor
data_location = r"C:\Users\marcus\Documents\_Working\DatEng_Capstone\data\table_mapping_zip_2_coor.parquet"
os.path.exists(data_location)
table_mapping_zip_2_coor = spark.read.parquet(data_location)
table_mapping_zip_2_coor.show()


# JOIN
cond = table_rentals_with_stations_having_NO_Parent.geo_plz == table_mapping_zip_2_coor.PLZ
table_rentals_with_coords = table_rentals_with_stations_having_NO_Parent.join(table_mapping_zip_2_coor, cond, "inner")
table_rentals_with_coords.show(50, truncate=True)

# FIND nearest
return



'''

def find_nearest_zipcode(self, row):
    """ Find the nearest zip code for a given data station by minimal distance of coordinates."""

    # a row describes a data_station
    station_para_lat = row["Geo_Breite"]
    station_para_long = row["Geo_Laenge"]

    # use already extracted numpy-arrays of coordinates for all zip codes,
    # and subtract the coordinate of a data station
    lat = self.lat_values - station_para_lat
    long = self.long_values - station_para_long

    # Calculate all distances for a given data station,
    # find the minimal distance, and return the corresponding zip code
    # as the nearest zip code for the data station.
    distance = abs(long) + abs(lat)
    nearest_location = self.mapping_zipcode_coordinates.iloc[np.argmin(distance)]

    return nearest_location

def get_nearest_zipcode(self):
    """Calculate the nearest zip code
     that has the minimal distance to a data station."""

    try:
        self.lat_values = self.mapping_zipcode_coordinates["Latitude"].values
        self.long_values = self.mapping_zipcode_coordinates["Longitude"].values

        self.data_stations[["Plz_matched", "Ort_matched", "Longitude_matched", "Latitude_matched"]] \
            = self.data_stations.apply(self.find_nearest_zipcode, axis=1)
        self.logger.debug("Mapped data_stations to zipcodes")
    except Exception:
        self.logger.exception("Can't map a data_station to a zipcode")


'''
