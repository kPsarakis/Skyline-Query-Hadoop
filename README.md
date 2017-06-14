# Skyline-Query-Hadoop
In this implementation of the algorithm we detect the skyline points in an apartment dataset for the purpose of rental with 2 or 3 dimensions(Price,Age,DistanceFromCityCenter), random or angular partitioning (the number of partitions are set by the user)

How to run the jar : hadoop jar {name of the jar} {main} {input} {output} #paritions random|angle 2d|3d

main = Skyline

input = input_file.csv (must exist in the hdfs input folder)

output = name of the output file (ex. skyline.csv) the output will be saved in the same folder as the jar 

