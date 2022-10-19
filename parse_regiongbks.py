"""
Parse all antiSMASH5+ regiongbks inside each genome folders
will store chunked TSV files of BGCs and CDS for further processing


TODO:
- test whether repartition will still works well in real clusters

require: pyspark, biopython

input:
[1] a folder of antiSMASH result files (one result folder per genome)
[2] output directory path (must not exist)
[3] num spark nodes to run

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType, ArrayType
import argparse
import glob
import subprocess
from os import path, makedirs
from multiprocessing import Pool

from Bio import SeqIO
from io import StringIO


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_folder", type=str, help="path to input folder")
    parser.add_argument("output_folder", type=str, help="path to output folder")
    parser.add_argument("--preprocessed_data", type=bool, default=False, help="is input data preprocessed using 'prepare_bgcs_data.py'?", action=argparse.BooleanOptionalAction)
    parser.add_argument("--num_partitions", type=int, default=0, help="set number of partitions")
    args = parser.parse_args()

    temp_folder = path.join(args.output_folder, "temp")

    if not args.output_folder.startswith("hdfs://"):
        if not path.exists(args.output_folder):
            makedirs(temp_folder)
        else:
            print("output folder exists!")
            return 1

    with SparkSession.builder\
        .getOrCreate() as spark:
        sc = spark.sparkContext        
        sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter",">>>split_gbk_file:#")

        if args.preprocessed_data:
            df = sc.textFile(path.join(args.input_folder, "part-*")).map(lambda x: tuple(x.split("\n", 1)) if x != "" else ("", ""))
        else:
            df = sc.wholeTextFiles(path.join(args.input_folder, "*/*/*.region*.gbk"))

        if args.num_partitions < 1:
            args.num_partitions = sc._jsc.sc().getExecutorMemoryStatus().keySet().size() * 4

        df = df.repartition(args.num_partitions)

        df_schema = StructType([
            StructField('dataset_name', StringType(), False),
            StructField('genome_name', StringType(), False),
            StructField('file_name', StringType(), False),
            StructField('feature_type', StringType(), False),
            StructField('feature_qualifiers', MapType(StringType(), ArrayType(StringType(), False), False), False),
            StructField('feature_start', LongType(), False),
            StructField('feature_end', LongType(), False)
        ])
        parsed_results = df\
            .flatMap(parse_bgcs)\
            .toDF(schema = df_schema)\
            .write.parquet(path.join(temp_folder, "parsed_rows"))

        sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter",">>>split_gbk_file:#")

        return 0


def parse_bgcs(tup):
    fp, gbk_text = tup

    if fp == "":
        return []

    results = []

    for record in SeqIO.parse(StringIO(gbk_text), "genbank"):
        dataset_name, genome_name, file_name = fp.split("/")[-3:]
        for feature in record.features:
            results.append((
                dataset_name,
                genome_name,
                file_name,
                feature.type,
                dict(feature.qualifiers),
                int(feature.location.start),
                int(feature.location.end)
            ))

    return results


if __name__ == "__main__":
    return_code = main()
    if return_code == 0:
        print("run complete!")
    else:
        print("run failed.")