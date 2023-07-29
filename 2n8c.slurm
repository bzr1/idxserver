#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-1:00:00

module load mpi4py/3.0.2-timed-pingpong
module load python/3.7.4
time mpiexec -n 4 python3 read_data.py
my-job-stats -a -n -s
