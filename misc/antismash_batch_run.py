"""
Run antismash in batch using Spark

NOTE:
- this will only use the driver's node, as the output folder
(and the antiSMASH executable) needs to be accessible by all nodes
and when running clusters with local[N], pySpark will use the same
environment as where it was called
- by default, each worker will use 1 CPU and 2GB RAM
- by default, antiSMASH will run with --minimal and --taxon bacteria parameters

TODO:
- make this actually portable and runnable on real cluster

require: pyspark

input:
[1] a folder of genome files in genbank (gbff, gbk, gb) or fasta (fasta, faa, fa)
[2] output directory path (must not exist)
[3] num spark nodes to run
[4] optional -- additional antismash parameters to use (use double-quotes)

"""


from pyspark.sql import SparkSession
from sys import argv
import glob
import subprocess
from os import path, makedirs


def main():
	input_folder = path.abspath(argv[1])
	output_folder = path.abspath(argv[2])
	num_threads = int(argv[3])
	if len(argv) > 4:
		additional_params = argv[4]
	else:
		additional_params = ""

	if not path.exists(output_folder):
		makedirs(output_folder)
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

		# fetch all input paths and count
		sc.setJobGroup("fetching_input_genomes", "fetching_input_genomes")
		to_process = sc.parallelize(glob.glob(path.join(input_folder, "*")))
		to_process = to_process.filter(
			lambda fp: path.basename(fp).rsplit(".", 1)[1] in [
					"gbff", "gb", "gbk", "fasta", "faa", "fa"
				]
			)
		num_input = to_process.count()
		print("found {:,} genomes, processing...".format(num_input))

		# repartition and process genomes
		sc.setJobGroup("processing_genomes", "processing_genomes")
		to_process = to_process.map(
			lambda fp: (
				fp,
				path.join(output_folder, path.basename(fp).rsplit(".", 1)[0]),
				additional_params
			)
		)
		to_process = to_process.map(run_antismash).collect()
		print("successfully processed {:,}/{:,} genomes".format(
			sum(to_process) / len(to_process)
		))
		return 0


def run_antismash(tup):
	input_path, output_path, additional_params = tup
	try:
	    subprocess.check_output(
	    	"antismash -c 1 --skip-sanitisation --minimal --taxon bacteria {} --output-dir {} {}".format(
	    		additional_params,
	    		output_path,
	    		input_path
	    	)
	    , shell=True)
	    return True
	except subprocess.CalledProcessError as e:
		return False


if __name__ == "__main__":
    return_code = main()
    if return_code == 0:
        print("run complete!")
    else:
        print("run failed.")