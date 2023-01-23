from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

import os
import sys

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './Code/src')

from utilities import utils


class CrashAnalysis:

    def __init__(self, path_to_config_file):
        input_file_paths = utils.read_yaml(path_to_config_file).get("INPUT_FILENAME")
        self.df_charges = utils.read_csv_data_to_df(spark, input_file_paths.get("Charges"))
        self.df_damages = utils.read_csv_data_to_df(spark, input_file_paths.get("Damages"))
        self.df_endorse = utils.read_csv_data_to_df(spark, input_file_paths.get("Endorse"))
        self.df_primary_person = utils.read_csv_data_to_df(spark, input_file_paths.get("Primary_Person"))
        self.df_units = utils.read_csv_data_to_df(spark, input_file_paths.get("Units"))
        self.df_restrict = utils.read_csv_data_to_df(spark, input_file_paths.get("Restrict"))

    def get_crashes_with_males_killed(self, output_path, output_format):

        """
        Analysis 1 - Number of crashes (accidents) in which number of persons killed are male
        :param output_path: output file path
        :param output_format: Write file format
        :return: contents of the DataFrame in Table Format
        """

        df = self.df_primary_person.distinct().filter((upper(col("PRSN_GNDR_ID"))=="MALE") & (upper(col("PRSN_INJRY_SEV_ID"))=="KILLED"))\
                            .select(countDistinct(col("CRASH_ID")).alias("CRASHES_WITH_MALES_KILLED"))
        utils.write_output(df, output_path, output_format)
        return df.show(truncate=False)


    def get_two_wheelers_booked(self, output_path, output_format):

        """
        Analysis 2 - Count of two wheelers booked for crashes
        :param output_path: output file path
        :param output_format: Write file format
        :return: contents of the DataFrame in Table Format
        """

        df = self.df_units.distinct().filter(upper(col("VEH_BODY_STYL_ID")).contains("MOTORCYCLE"))\
             .select(countDistinct(col("VIN")).alias("TWO_WHEELERS_BOOKED_FOR_CRASHES"))
        utils.write_output(df, output_path, output_format)

        return df.show(truncate=False)

    def get_state_with_highest_female_crashes(self, output_path, output_format):

        """
        Analysis 3 - State having highest number of accidents in which females are involved
        :param output_path: output file path
        :param output_format: Write file format
        :return: contents of the DataFrame in Table Format
        """

        df = self.df_primary_person.distinct().filter((upper(col("PRSN_GNDR_ID"))=="FEMALE") & (~upper(col("DRVR_LIC_STATE_ID")).isin("NA", "UNKNOWN")))\
                     .groupBy(col("DRVR_LIC_STATE_ID")).agg(countDistinct(col("CRASH_ID")).alias("TOTAL_CRASH")).orderBy(col("TOTAL_CRASH").desc()) \
                     .select(col("DRVR_LIC_STATE_ID").alias("STATE_HAVING_HIGHEST_ACCIDENTS_WITH_FEMALE_INVOLVED")).limit(1)
        utils.write_output(df, output_path, output_format)

        return df.show(truncate=False)
    

    def get_top_vehicle_contributing_injuries(self, output_path, output_format):

        """
        Analysis 4 - Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        :param output_path: output file path
        :param output_format: Write file format
        :return: contents of the DataFrame in Table Format
        """

        df = self.df_units.distinct().filter(upper(col("VEH_MAKE_ID")) != "NA"). \
            withColumn('TOTAL_INJURIES_DEATH_CNT', self.df_units[35] + self.df_units[36]). \
            groupBy("VEH_MAKE_ID").sum("TOTAL_INJURIES_DEATH_CNT"). \
            withColumnRenamed("sum(TOTAL_INJURIES_DEATH_CNT)", "TOTAL_INJURIES_DEATH_CNT_AGG"). \
            orderBy(col("TOTAL_INJURIES_DEATH_CNT_AGG").desc())
        
        window_spec_veh_make = Window.orderBy(col("TOTAL_INJURIES_DEATH_CNT_AGG").desc())
        df = df.withColumn("rank", rank().over(window_spec_veh_make)).select(col("VEH_MAKE_ID"), col("RANK")).filter( (col("rank")>= 5) & (col("rank")<= 15))
        utils.write_output(df, output_path, output_format)

        return df.show(truncate=False)

    def get_top_ethnic_group_for_body_style(self, output_path, output_format):

        """
        Analysis 5 - Top ethnic user group of each unique body style
        :param output_path: output file path
        :param output_format: Write file format
        :return: contents of the DataFrame in Table Format
        """

        w = Window.partitionBy(col("VEH_BODY_STYL_ID")).orderBy(col("count").desc())
        df_primary_person_distinct= self.df_primary_person.distinct()
        df_units_distinct= self.df_units.distinct()
        df = df_units_distinct.join(df_primary_person_distinct, "CRASH_ID", how='inner'). \
            filter(~upper(col("VEH_BODY_STYL_ID")).isin(["NA", "UNKNOWN", "NOT REPORTED",
                                                         "OTHER  (EXPLAIN IN NARRATIVE)"])). \
            filter(~upper(col("PRSN_ETHNICITY_ID")).isin(["NA", "UNKNOWN"])). \
            groupby(col("VEH_BODY_STYL_ID"), col("PRSN_ETHNICITY_ID")).count(). \
            withColumn("row", row_number().over(w)).filter(col("row") == 1).drop("row", "count")
        
        utils.write_output(df, output_path, output_format)
        
        return df.show(truncate=False)

    def get_top_5_zip_codes_with_alcohol_as_cf_for_crash(self, output_path, output_format):

        """
         Analysis 6 -  Among the crashed cars, Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash
        :param output_path: output file path
        :param output_format: Write file format
        :return: contents of the DataFrame in Table Format
        """

        car_crash_check = instr(upper(col("VEH_BODY_STYL_ID")) , "CAR") >=1
        df_units_crash_include_car = self.df_units.withColumn("CAR_FLG", car_crash_check)\
                    .filter(col("CAR_FLG")).select(col("CRASH_ID")).distinct()

        win_spec = Window.orderBy(col("NUM_TOT_CRASHES").desc())
        df = self.df_primary_person.join(df_units_crash_include_car, "CRASH_ID", "inner").where((upper(col("PRSN_ALC_RSLT_ID")) == "POSITIVE") & (col("DRVR_ZIP").isNotNull()))\
              .groupBy(col("DRVR_ZIP")).agg(countDistinct("CRASH_ID").alias("NUM_TOT_CRASHES"))\
              .withColumn("rank", rank().over(win_spec))

        df = df.where(col("rank")<=5).select(col("DRVR_ZIP"), col("RANK"))

        utils.write_output(df, output_path, output_format)

        return df.show(truncate=False)

    def get_crash_ids_with_no_damage_property(self, output_path, output_format):

        """
        Analysis 7 - Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 
        and car avails Insurance
        :param output_path: output file path
        :param output_format: write file format
        :return: contents of the DataFrame in Table Format
        """

        insurance_check = instr(col("FIN_RESP_TYPE_ID") , "INSURANCE") >=1

        df_units_ins_dmg = self.df_units.distinct()\
        .withColumn("VEH_DMG_SCL_1",regexp_extract(col('VEH_DMAG_SCL_1_id'), r'(\d+)', 1).cast('bigint'))\
        .withColumn("VEH_DMG_SCL_2",regexp_extract(col('VEH_DMAG_SCL_2_id'), r'(\d+)', 1).cast('bigint'))\
        .withColumn("INSURANCE_FLG", insurance_check)\
        .select(col("CRASH_ID"),col('VEH_DMG_SCL_1'), col("VEH_DMG_SCL_2"), col("INSURANCE_FLG"),col("FIN_RESP_PROOF_ID"))\
        .filter((~col("FIN_RESP_PROOF_ID").isin("NA","NR")) & ((col("VEH_DMG_SCL_1") >4) | (col("VEH_DMG_SCL_2") >4) ) & (col("INSURANCE_FLG")))

        df_damage_filter = self.df_damages.distinct().filter(~upper(col("DAMAGED_PROPERTY")).isin("NONE", "NONE1"))
        df = df_units_ins_dmg.join(df_damage_filter, "CRASH_ID", "leftanti")

        df = df.select(countDistinct(col('CRASH_ID')).alias('DISTINCT_CRASH_IDS_WITH_NODAMAGEPROPERTY'))

        utils.write_output(df, output_path, output_format)

        return df.show(truncate=False)

    def get_top_5_vehicle_makes(self, output_path, output_format):

        """
        Analysis 8 - Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and 
        has car licensed with the Top 25 states with highest number of offences
        :param output_path: output file path
        :param output_format: write file format
        :return: contents of the DataFrame in Table Format
        """

        top_25_state = [row[0] for row in self.df_units.distinct().filter(col("VEH_LIC_STATE_ID").cast("int").isNull())\
            .groupBy("VEH_LIC_STATE_ID").agg(countDistinct(col("CRASH_ID")).alias("Distinct_CNT")).orderBy(col("Distinct_CNT").desc()).limit(25).collect()]
        top_10_used_colors = [row[0] for row in self.df_units.distinct().filter(self.df_units.VEH_COLOR_ID != "NA")\
            .groupBy("VEH_COLOR_ID").agg(countDistinct(col("CRASH_ID")).alias("Distinct_CNT")).orderBy(col("Distinct_CNT").desc()).collect()]

        df_charges_filter = self.df_charges.distinct()
        df_primary_person_filter = self.df_primary_person.distinct()
        df_units_filter = self.df_units.distinct()
        df = df_charges_filter.join(df_primary_person_filter, on=['CRASH_ID'], how='inner') \
            .join(df_units_filter, on=['CRASH_ID'], how='inner') \
            .filter(col("CHARGE").contains("SPEED")) \
            .filter(col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])) \
            .filter(col("VEH_COLOR_ID").isin(top_10_used_colors)) \
            .filter(col("VEH_LIC_STATE_ID").isin(top_25_state)) \
            .groupBy("VEH_MAKE_ID").count() \
            .orderBy(col("count").desc()).limit(5).select(col("VEH_MAKE_ID"))

        utils.write_output(df, output_path, output_format)

        return df.show(truncate=False)


if __name__ == '__main__':
    # Initialize sparks session
    spark = SparkSession \
        .builder \
        .appName("CrashAnalysis") \
        .getOrCreate()

    config_file_path = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    crash = CrashAnalysis(config_file_path)
    output_file_paths = utils.read_yaml(config_file_path).get("OUTPUT_PATH")
    file_format = utils.read_yaml(config_file_path).get("FILE_FORMAT")

    # Adding space between executions and results
    print("\n")

    # ANALYSIS 1 - Find the number of crashes (accidents) in which number of persons killed are male? 
    print("Analysis 1 - Number of crashes in which number of persons killed are male")
    crash.get_crashes_with_males_killed(output_file_paths.get(1), file_format.get("Output"))

    # ANALYSIS 2 - How many two wheelers are booked for crashes? 
    print("Analysis 2 - Number of Two wheelers booked for crashes")
    crash.get_two_wheelers_booked(output_file_paths.get(2), file_format.get("Output"))

    # ANALYSIS 3 - Which state has highest number of accidents in which females are involved?
    print("Analysis 3 - State having highest accidents with females involved")

    crash.get_state_with_highest_female_crashes(output_file_paths.get(3),file_format.get("Output"))

    # ANALYSIS 4 - Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death?
    print("Analysis 4 - Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death")
    crash.get_top_vehicle_contributing_injuries(output_file_paths.get(4),file_format.get("Output"))

    # ANALYSIS 5 - For all the body styles involved in crashes, mention the top ethnic user group of each unique body style?
    print("ANALYSIS 5 - Top ethnic group for each body style")
    crash.get_top_ethnic_group_for_body_style(output_file_paths.get(5), file_format.get("Output"))

    # ANALYSIS 6 - Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the 
    # contributing factor to a crash (Use Driver Zip Code)?
    print("ANALYSIS 6 - Top 5 zipcodes with alcohols as contribution factor for crash") 
    crash.get_top_5_zip_codes_with_alcohol_as_cf_for_crash(output_file_paths.get(6),file_format.get("Output"))

    # ANALYSIS 7 - Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 
    # and car avails Insurance?
    print("ANALYSIS 7 - Crash IDs Count where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance") 
    crash.get_crash_ids_with_no_damage_property(output_file_paths.get(7), file_format.get("Output"))

    # ANALYSIS 8 - Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, 
    # used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences 
    # (to be deduced from the data)?
    print("ANALYSIS 8 - Top 5 Vehicle Makes where drivers are charged with speeding related offences") 
    crash.get_top_5_vehicle_makes(output_file_paths.get(8), file_format.get("Output"))

    spark.stop()
