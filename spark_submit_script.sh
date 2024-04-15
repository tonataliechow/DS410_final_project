#!/bin/bash
#SBATCH --job-name=spark_job          # Job name
#SBATCH --nodes=16                     # Number of nodes to request
#SBATCH --ntasks-per-node=4          # Number of processes per node
#SBATCH --mem=16G                      # Memory per node
#SBATCH --time=8:00:00                # Maximum runtime in HH:MM:SS
#SBATCH --account=open 	      # Queue
#SBATCH --mail-user=trn5106@psu.edu
#SBATCH --mail-type=BEGIN

# Load necessary modules (if required)
module load anaconda3
source activate ds410_sp24
module use /gpfs/group/RISE/sw7/modules
module load spark/3.3.0
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Run PySpark
# Record the start time
start_time=$(date +%s)
spark-submit --deploy-mode client FINAL_DS410_Project_Cluster.py

#python Lab11.py

# Record the end time
end_time=$(date +%s)

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time: $execution_time seconds"
