#!/bin/bash
#SBATCH --mail-type=ALL
#SBATCH --mail-user=m.a.schepers@st.hanze.nl
#SBATCH --time 48:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --job-name=assignment4_maschepers
#SBATCH --partition=assemblix
mkdir -p output_dir

export FILE1=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R1_001_BC24EVACXX.filt.fastq
export FILE2=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R2_001_BC24EVACXX.filt.fastq
export OUTPUT=/students/2021-2022/master/DaanSteur_DSLS

# Run velveth and store output in output folder 
parallel -j10 'velveth ${OUTPUT}/output_${} ${} -fastq -short $FILE1 -shortPaired $FILE2' ::: 23 25
# Run velvetg
parallel -j10 'velvetg ${OUTPUT}/output_${} -exp_cov auto -cov_cutoff auto' ::: 23 25

# Run python file and save output
# python assignment4.py