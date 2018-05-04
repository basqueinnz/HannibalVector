import logging
from datetime import datetime
import time
import re
import glob
import os.path
from multiprocessing.dummy import Pool  # This is a thread-based Pool
from multiprocessing import cpu_count
from random import randint

import boto3
from boto3.s3.transfer import S3Transfer

import scrape_movies
import scrape_keywords_for_top_movies as scrape_keywords
import create_model


def scrap_last_year_and_create_model():
    yearToday = datetime.now().today().year

    print_line_for_user("** Updating the model for year: {} **".format(yearToday))

    measure_and_print_elapsed_time(lambda: scrape_movies.scrap_year(yearToday), "- Scrapping movies")

    measure_and_print_elapsed_time(create_model.create, "- Starting model creation, this would take few minutes.")


def scrap_and_create_model_from_scratch():
    concurrentYears = 10

    yearToday = datetime.now().today().year
    allYears = range(1940, yearToday)

    totalAmountOfIterations = round(len(allYears)/concurrentYears)

    for iteration in range(totalAmountOfIterations):
        startYear = iteration * concurrentYears
        endYear = startYear + concurrentYears
        years = allYears[startYear: endYear]

        yearsDescription = " ".join(str(year) for year in years)
        print_line_for_user(
            "Updating the model for year(s): " + yearsDescription)
        print_line_for_user("-------------------------------------")

        print_line_for_user("- Scrapping movies")
        startProcessingTime = time.time()
        threadCount = len(years)  # or cpu_count() * 2
        logging.debug(
            "Creating {} threads to parallelize".format(str(threadCount)))
        pool = Pool(threadCount)
        pool.map(scrape_movies.scrap_year, years)
        endProcessingTime = time.time()
        elapsedTimeInSeconds = endProcessingTime - startProcessingTime
        print_line_for_user("Completed in: %ss" % elapsedTimeInSeconds)

        print_line_for_user("- Scrapping movie keywords")
        startProcessingTime = time.time()
        threadCount = cpu_count() * 2
        logging.debug(
            "Creating {} threads to parallelize".format(str(threadCount)))
        pool = Pool(threadCount)
        pool.map(scrape_keywords.scrap_year, years)
        endProcessingTime = time.time()
        elapsedTimeInSeconds = endProcessingTime - startProcessingTime
        print_line_for_user("Completed in: %ss" % elapsedTimeInSeconds)

        # Sleeping for a while to avoid throwling from IMDb
        if(iteration != totalAmountOfIterations - 1):
            time.sleep(randint(40, 75))

    measure_and_print_elapsed_time(
        create_model.create, "- Starting model creation, this would take few minutes.")


def measure_and_print_elapsed_time(function_to_run, starting_message):
    print_line_for_user(starting_message)
    startProcessingTime = time.time()

    function_to_run()

    endProcessingTime = time.time()
    elapsedTimeInSeconds = endProcessingTime - startProcessingTime
    print_line_for_user("Completed in: %ss" % elapsedTimeInSeconds)


def ring_the_bell():
    print("\a")


def get_compressed_files_order_by_last_modified(file_name_contains):
    list_of_files = glob.glob("*{}*.gz".format(file_name_contains))
    return sorted(list_of_files, key=os.path.getctime, reverse=True)


def upload_model_to_s3():
    bucket_name = "hannibal-vector"

    print_line_for_user("- Starting uploading model and index files")

    index_file_name = get_compressed_files_order_by_last_modified("Index")[0]
    model_file_name = get_compressed_files_order_by_last_modified("Model")[0]

    logging.info("Uploading to Bucket name: {}".format(bucket_name))
    client = boto3.client("s3")
    transfer = S3Transfer(client)
    transfer.upload_file(index_file_name, bucket_name, index_file_name)
    transfer.upload_file(model_file_name, bucket_name, model_file_name)
    print_line_for_user("Upload completed!")


def print_line_for_user(text):
    logging.warn("\t" + text)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)

    print_line_for_user("-------------------------------------")

    scrap_last_year_and_create_model()
    #scrap_and_create_model_from_scratch()
    upload_model_to_s3()

    print_line_for_user(" ----- ALL TASKS COMPLETED! -----")
    print_line_for_user("-------------------------------------")
    ring_the_bell()
