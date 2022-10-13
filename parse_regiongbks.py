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
from sys import argv
import glob
import subprocess
from os import path, makedirs
from multiprocessing import Pool

from Bio import SeqIO
from io import StringIO


def main():
    input_folder = path.abspath(argv[1])
    output_folder = path.abspath(argv[2])
    temp_folder = path.join(output_folder, "temp")
    num_threads = int(argv[3])

    if not path.exists(output_folder):
        makedirs(temp_folder)
    else:
        print("output folder exists!")
        return 1

    sc_master = "local[{}]".format(num_threads)
    with SparkSession.builder\
        .master(sc_master)\
        .config("spark.executor.cores", "1")\
        .config("spark.executor.memory", "2G")\
        .getOrCreate() as spark:
        sc = spark.sparkContext

        pattern_bgc_files = path.join(input_folder, "*/*.region*.gbk")

        # get number of partitions
        num_files = sum([True for i in glob.iglob(pattern_bgc_files)])
        num_partitions = max(num_threads, (num_files // 100) + 1)

        def combine_texts(pidx, txts):
            genome_names = []
            genbank_text = ""
            for filepath, text in txts:
                genome_names.append(filepath.split("/")[-2])
                genbank_text += text
            yield (pidx, genome_names, genbank_text)

        parsed_results = sc.wholeTextFiles(pattern_bgc_files, minPartitions=num_partitions)\
                .mapPartitionsWithIndex(combine_texts)\
                .map(parse_bgcs)\
                .repartition(num_threads)\
                .map(lambda row: (temp_folder, row[0], row[1], row[2]))\
                .toLocalIterator()

        for tup in parsed_results:
            store_parsed_bgcs(tup)

        return 0


def store_parsed_bgcs(tup):
    output_folder, pidx, result_bgcs, result_cds = tup
    with open(path.join(output_folder, "bgc-{}.tsv".format(pidx)), "w") as oo:
        for row in result_bgcs:
            oo.write("{}\n".format("\t".join(map(str, row))))
    with open(path.join(output_folder, "cds-{}.tsv".format(pidx)), "w") as oo:
        for row in result_cds:
            oo.write("{}\n".format("\t".join(map(str, row))))


def parse_bgcs(tup):
    pidx, genome_names, gbk_text = tup
    
    result_bgcs = []
    result_cds = []

    for record in SeqIO.parse(StringIO(gbk_text), "genbank"):
        genome_name = genome_names.pop(0)
        for feature in record.features:
            if feature.type == "region":
                is_complete = feature.qualifiers["contig_edge"][0] == "False"
                region_num = feature.qualifiers["region_number"][0]
                bgc_class = ";".join(set(feature.qualifiers["product"]))
                break
        result_bgcs.append([
            genome_name,
            record.id,
            region_num,
            bgc_class,
        ])
        cds_num = 1
        for feature in record.features:
            if feature.type == "CDS":
                result_cds.append([
                    genome_name,
                    record.id,
                    region_num,
                    cds_num,
                    feature.location.start,
                    feature.location.end
                ])
                cds_num += 1

    return (pidx, result_bgcs, result_cds)


if __name__ == "__main__":
    return_code = main()
    if return_code == 0:
        print("run complete!")
    else:
        print("run failed.")