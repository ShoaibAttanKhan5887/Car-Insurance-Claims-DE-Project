import geopy
import pandas as pd
from pyspark.sql.functions import col, lit, concat, pandas_udf, avg
from typing import Iterator
import random
from pyspark import pipelines as dp

catalog = 'smart-claims-dev'
silver_schema = '02_silver'
gold_schema = '03_gold'

def geocode(geolocator, address):
    try:
        location = geolocator.geocode(address)
        if location:
            return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
        print(f"error getting lat/long: {e}")
    return pd.Series({'latitude': None, 'longitude': None})
      
@pandas_udf("latitude float, longitude float")
def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    import ssl
    import certifi
    ctx = ssl.create_default_context(cafile=certifi.where())
    geopy.geocoders.options.default_ssl_context = ctx
    geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https')
    for address in batch_iter:
        yield address.apply(lambda x: geocode(geolocator, x))

@dp.table(
    name=f"`{catalog}`.{gold_schema}.aggregated_car_telematics",
    comment="Average telematics",
    table_properties={
        "quality": "gold"
    }
)
def telematics():
    return (
        spark.read.table(f"`{catalog}`.{silver_schema}.car_telematics")
        .groupBy("chassis_no")
        .agg(
            avg("speed").alias("telematics_speed"),
            avg("latitude").alias("telematics_latitude"),
            avg("longitude").alias("telematics_longitude"),
        )
    )

# --- CLAIM-POLICY ---
@dp.table(
    name=f"`{catalog}`.{gold_schema}.customer_claim_policy",
    comment = "Curated claim joined with policy records",
    table_properties={
        "quality": "gold"
    }
)
def customer_claim_policy():
    # Read the cleaned policy records
    policy = spark.readStream.table(f"`{catalog}`.{silver_schema}.policy")
    # Read the cleaned claim records
    claim = spark.readStream.table(f"`{catalog}`.{silver_schema}.claim")
    # Read the cleaned customer records
    customer = spark.readStream.table(f"`{catalog}`.{silver_schema}.customer") 
    claim_policy = claim.join(policy, "policy_no")
    return claim_policy.join(customer, claim_policy.cust_id == customer.customer_id)

# --- CLAIM-POLICY-TELEMATICS ---
@dp.table(
    name=f"`{catalog}`.{gold_schema}.customer_claim_policy_telematics",
    comment="claims with geolocation latitude/longitude",
        table_properties={
        "quality": "gold"
    }
)
def customer_claim_policy_telematics():
  telematics = spark.read.table(f"`{catalog}`.{gold_schema}.aggregated_car_telematics")
  customer_claim_policy = spark.readStream.table(f"`{catalog}`.{gold_schema}.customer_claim_policy").where("BOROUGH is not null")
  return (customer_claim_policy
            .withColumn("lat_long", get_lat_long(col("address")))
            .join(telematics, on="chassis_no")
        )