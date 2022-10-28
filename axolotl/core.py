"""axolotl.core

Contain core classes and functions
"""

from pyspark.sql import DataFrame, Row, types
from pyspark.sql import SparkSession

from abc import ABC, abstractmethod
from os import path
import json


class AxolotlDF(ABC):
    """Axoltl basic DataFrame class"""

    def __init__(self, df:DataFrame):
        if not self.getSchema() == None and self.__class__.getSchema().jsonValue() != df.schema.jsonValue():
            raise AttributeError((
                "schema conflict on the loaded DataFrame object,"
                " please use schema={}.getSchema() when creating the"
                " pySpark DataFrame object"
            ).format(
                self.__class__.__name__
            ))
        self.df = df
    
    def getMetadata(self) -> dict:
        metadata = {
            "class_name": self.__class__.__name__,
            "schema": self.df.schema.jsonValue()
        }
        return metadata

    @classmethod
    def readParquet(cls, src_parquet:str, num_partitions:int=-1):
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")        

        metadata_path = path.join(src_parquet, ".axolotl_metadata.json")
        if not path.exists(metadata_path):
            raise FileNotFoundError("can't find axolotl_metadata.json!")
        else:
            with open(metadata_path) as infile:
                metadata = json.load(infile)
            if metadata["class_name"] != cls.__name__:
                raise TypeError("trying to load {} parquet file into a {}".format(
                    metadata["class_name"],
                    cls.__name__
                ))
            if not cls.getSchema() == None and cls.getSchema().jsonValue() != metadata["schema"]:
                raise AttributeError("schema conflict on the loaded parquet file")
        
        if num_partitions > 0:
            return cls(spark.read.schema(cls.getSchema()).parquet(src_parquet).repartition(num_partitions))
        else:
            return cls(spark.read.schema(cls.getSchema()).parquet(src_parquet))
    
    def writeParquet(self, parquet_file_path:str, num_partitions:int=-1):
        if path.exists(parquet_file_path):
            raise Exception("path exists! {}".format(parquet_file_path))
        if num_partitions > 0:
            self.df.repartition(num_partitions).write.option("schema", self.__class__.getSchema()).parquet(parquet_file_path)
        else:
            self.df.write.option("schema", self.__class__.getSchema()).parquet(parquet_file_path)
        metadata_path = path.join(parquet_file_path, ".axolotl_metadata.json")        
        with open(metadata_path, "w") as outfile:
            outfile.write(json.dumps(self.getMetadata()))
    
    @classmethod
    @abstractmethod
    def getSchema(cls) -> types.StructType:
        """return: DF schema"""
        raise NotImplementedError("calling an unimplemented abstract method getSchema()")
    
    @classmethod
    @abstractmethod
    def validateRow(cls, row: Row) -> bool:
        """return: validated/not"""
        raise NotImplementedError("calling an unimplemented abstract method validateRow()")
    
    def filterValids(self) -> DataFrame:
        return self.__class__(
            self.df.rdd\
            .filter(self.__class__.validateRow)\
            .toDF(schema=self.__class__.getSchema())
        )

    @classmethod
    def validateRowNot(cls, row: Row) -> bool:
        return not cls.validateRow(row)
    
    def filterNotValids(self) -> DataFrame:
        return self.__class__(
            self.df.rdd\
            .filter(self.__class__.validateRowNot)\
            .toDF(schema=self.__class__.getSchema())
        )
                
    def countValids(self) -> tuple[int, int]:
        return self.df.rdd.map(self.__class__.validateRow).aggregate(
            (0, 0),
            lambda x, y: (x[0] + 1, x[1]) if y else (x[0], x[1] + 1),
            lambda x, y: (x[0] + y[0], x[1] + y[1])
        )