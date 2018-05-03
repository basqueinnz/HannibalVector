from datetime import datetime
import time
import re
import os.path
from multiprocessing.dummy import Pool  # This is a thread-based Pool
from multiprocessing import cpu_count

import scrape_movies
import scrape_keywords_for_top_movies as scrape_keywords
import create_model


def scrap_and_create_model():
    now = datetime.now()
    today = now.today()
    yearToday = now.today().year

    years = range(2010, 2019)
    print(years)
    yearsDescription = " ".join(str(year) for year in years)
    print("Updating the model for year(s): " + yearsDescription)
    print("-------------------------------------")

    print("- Scrapping movies")
    startProcessingTime = time.time()
    threadCount = len(years) # or cpu_count() * 2
    print("Creating {} threads to parallelize".format(str(threadCount)))
    pool = Pool(threadCount)
    pool.map(scrape_movies.scrap_year, years)
    endProcessingTime = time.time()
    elapsedTimeInSeconds = endProcessingTime - startProcessingTime
    print("Completed in: %ss" % elapsedTimeInSeconds)

    print("- Scrapping movie keywords")
    startProcessingTime = time.time()
    threadCount = cpu_count() * 2
    print("Creating {} threads to parallelize".format(str(threadCount)))
    pool = Pool(threadCount)
    pool.map(scrape_keywords.scrap_year, years)
    endProcessingTime = time.time()
    elapsedTimeInSeconds = endProcessingTime - startProcessingTime
    print("Completed in: %ss" % elapsedTimeInSeconds)

    print("- Starting model creation, this would take few minutes.")
    create_model.create()


if __name__ == "__main__":
    scrap_and_create_model()
