import timeit #find the execution time taken by the given code snippet
import pyspark.sql.functions as f
import uuid #128-bit number used to uniquely identify information in computer systems
import random #generates random floating numbers in the range of 0.1, and 1.0
import string #The Python string module provides several constants that are useful for checking to see if a character, slice, or string contains letters, digits, symbols, etc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from datetime import datetime, timedelta
import pyspark
from pyspark.sql import SparkSession
import os


class TestDataGen:
    def __init__(self):
        self.spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

    def generate_name(self):
        length=5
        VOWELS = "aeiou"
        CONSONANTS = ''.join(set(string.ascii_lowercase) - set(VOWELS))
        word = ""
        
        for i in range(length):
            if i % 2 == 0:
                word += random.choice(CONSONANTS)
            else:
                word += random.choice(VOWELS)
        
        return word
        
    def datagen(self,spark):
        
        for i in range(1,100):
            data= [({"order_id":''.join(random.choice(string.ascii_letters) for i in range(8)),
                "customer_order_id":''.join(random.choice(string.ascii_letters + string.digits) for i in range(12)),
                "tracking_number":''.join(random.choice(string.digits) for i in range(10)),
                "Part_name":f"{self.generate_name()}",
                "Quantity":random.randint(1,20),
                "cost":random.randint(1,999),
                "Pack_Date":datetime(2022, 1, 1) + timedelta(days=random.randint(0, (datetime(2024, 12, 31) - datetime(2022, 1, 1)).days)).date(),
                "Delivery_Date":datetime(2022, 1, 1) + timedelta(days=random.randint(0, (datetime(2024, 12, 31) - datetime(2022, 1, 1)).days)).date(), 
                "Phone":''.join(random.choice(string.digits) for i in range(10)),
                "Email": f"{''.join(random.choices(string.ascii_letters + string.digits, k=8))}@example.com",
                "Pincode":''.join(random.choices(string.ascii_uppercase + string.digits, k=3)) + ' ' + ''.join(random.choices(string.digits + string.ascii_uppercase, k=3))})]
            
 
            df = spark.createDataFrame(data)
            df.write.mode("append").format("delta").option("overwriteSchema", "true").save("test_table5")
