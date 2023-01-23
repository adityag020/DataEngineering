## Case Study:

### Analytics: 
Application should perform below analysis and store the results for each analysis.
* Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
* Analysis 2: How many two-wheelers are booked for crashes? 
* Analysis 3: Which state has the highest number of accidents in which females are involved? 
* Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
* Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
* Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
* Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
* Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

### Expected Output:
1. Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
2. Code should be properly organized in folders as a project.
3. Input data sources and output should be config driven
4. Code should be strictly developed using Dataframe APIs (Do not use Spark SQL)
5. Upload the entire project in Github repo.

### Process to follow:
1. Create virtual environment and install all packages and dependencies (if using VS Code).
2. Go to Project Directory: `$ cd BCG_DataEngineering_CaseStudy`
3. In Bash terminal, run `$ make`. It will run all the commands in Makefile and build the project to run via spark-submit. In this process, a  new folder with name "dist" will be created, and the code artefacts will be copied into it.
4. Using Dist, src.zip file as an argument with py files will be submitted to spark-submit to run against the cluster.
5. In CLI, run `$ cd Dist && spark-submit --master "local[*]" --py-files src.zip --files config.yaml main.py && cd ..`

##### NOTE: VS Code screenshots are attached.
