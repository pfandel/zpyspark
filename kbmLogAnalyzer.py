#
# To Run:
#
# > spark-submit --conf spark.ui.port=4949 kbmLogAnalyzer.py
#
# Before running delete the data folders created by previous execution
# The cleanup.sh script  can be used for this.
# Comment 2

# Import required libraries
from pyspark import SparkContext
import os

# Create entry point for Spark functionality
sc = SparkContext("local", "Simple App")

# Load data from input file into RDD
logFile = "afoServer.log"  # Should be some file on your system
rawData = sc.textFile(logFile).cache()

# Create another RDD containing only the warning messages, filter
# out those relating to running with dev environment
warningData = rawData.filter(lambda line: " WARNING: " in line)
warningData = warningData.filter (lambda line: "probably not a mib .\mibs\CVS" not in line)

# Remove the timestamp from each warning line (by keeping only characters after '] ')
#warningData = warningData.map(lambda line: line[15:])
warningData = warningData.map(lambda line: line[(line.find("]")+2):])

# Print total warning count and export all warnings to file
print("%i warnings in log" % (warningData.count()))
warningData.saveAsTextFile("afoServerWarnings.log")

# Print distinct warning count and export all distinct warnings to file
warningData = warningData.distinct()
print("%i distinct warnings in log" % (warningData.count()))
warningData.saveAsTextFile("afoServerDistinctWarnings.log")

# Sort the warning lines (by 1st 20 characters) and export to file
warningData = warningData.sortBy(lambda line: line[:20])
warningData.saveAsTextFile("afoServerSortedDistinctWarnings.log")
