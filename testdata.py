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
        
        for i in range(1,10):
            '''i= [({"order_id":''.join(random.choice(string.ascii_letters) for i in range(8)),
                    "customer_order_id":''.join(random.choice(string.ascii_letters + string.digits) for i in range(12)),
                    "tracking_number":''.join(random.choice(string.digits) for i in range(10)),
                    "Part_name":f"{self.generate_name()}",
                    "Quantity":random.randint(1,20),
                    "cost":random.randint(1,999),
                    "Pack_Date":datetime(2022, 1, 1) + timedelta(days=random.randint(0, (datetime(2024, 12, 31) - datetime(2022, 1, 1)).days)),
                    "Delivery_Date":datetime(2022, 1, 1) + timedelta(days=random.randint(0, (datetime(2024, 12, 31) - datetime(2022, 1, 1)).days)), 
                    "Phone":''.join(random.choice(string.digits) for i in range(10)),
                    "Email":random.choice(f"{''.join(random.choices(string.ascii_letters + string.digits, k=8))}@example.com"),
                    "Pincode":''.join(random.choices(string.ascii_uppercase + string.digits, k=3)) + ' ' + ''.join(random.choices(string.digits + string.ascii_uppercase, k=3))})]'''
            
            data= [({"order_id":random.choice(["code","kate"]),
                "customer_order_id":random.choice(["code","kate"]),
                "tracking_number":random.choice(["code","kateykin"]),
                "Part_name":generate_name(),
                "Quantity":random.choice(["code","kate"]),
                "cost":random.choice(["code","kate"]),
                "Pack_Date":random.choice(["code","kate"]),
                "Delivery_Date":random.choice(["code","kate"]), 
                "Phone":random.choice(["code","kate"]),
                "Email":random.choice(["code","king"]),
                "Pincode":random.choice(["code","kate"])})]
            df = spark.createDataFrame(data)
            df.write.mode("append").format("delta").saveAsTable("test_table5")
