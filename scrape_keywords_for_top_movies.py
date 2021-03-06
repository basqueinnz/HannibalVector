import bs4
import argparse
import urllib.request
import pandas as pd
import logging
import os
from multiprocessing.dummy import Pool  # This is a thread-based Pool
from multiprocessing import cpu_count


def get_input_from_user():
    PARSER = argparse.ArgumentParser(description='This tool will create csv files of movies scraped from IMDb.com')

    PARSER.add_argument('start', help='Year to start scraping from', type=int)
    PARSER.add_argument('end', help='Year to start scraping at', type=int)

    # TODO: Validate the years (not lower than <1900 or higher than 2100)
    ARGS = PARSER.parse_args()

    years = range(ARGS.start, ARGS.end + 1)

    logging.basicConfig(level=logging.INFO)
    logging.info('Scraping keyword data for top movies from %d to %d.' % (ARGS.start, ARGS.end + 1))

    return years


def write_to_csv(movies_list, year):
    df = pd.DataFrame(movies_list)
    file_name = 'scraped_movies/keywords_for_top_movies_of_%d.csv' % year
    df.to_csv(file_name, index=False, encoding="utf-8")
    logging.info('Wrote the following file: %s' % file_name)

def scrap_keywords_from_imd_with_id(IMDbId):
    logging.info('Fetching keywords for %s' % IMDbId)

    url = 'http://www.imdb.com/title/%s/keywords' % IMDbId
    with urllib.request.urlopen(url) as response:
        html = response.read()
    soup = bs4.BeautifulSoup(html, "lxml", from_encoding="utf-8")
    keyword_tags = soup.find_all(attrs={'class': "soda sodavote"})
    keywords = '|'.join([i['data-item-keyword'] for i in keyword_tags])
    movie_data = {'IMDbId': IMDbId,
                    'keywords': keywords}
    return movie_data


def scrap_year(year):
    movies_file_name = './scraped_movies/top_movies_of_%d.csv'
    movies = pd.read_csv(movies_file_name % year, encoding = "utf-8")
    keywords_list = []

    concurrentCalls = 15
    totalAmountOfIterations = round(len(movies.IMDbId)/concurrentCalls)
    for iteration in range(totalAmountOfIterations):
        IMDbIds = movies[iteration * concurrentCalls:(iteration + 1) * concurrentCalls].IMDbId
        logging.debug("Creating {} threads to parallelize" % str(threadCount))
        pool = Pool(concurrentCalls)
        movies_data = pool.map(scrap_keywords_from_imd_with_id, IMDbIds)
        keywords_list.extend(movies_data)

    write_to_csv(keywords_list, year)
    logging.info('Wrote file for %d' % year)


if __name__ == '__main__':
    years = get_input_from_user()

    threadCount = cpu_count() * 2
    logging.debug("Creating {} threads to parallelize" % str(threadCount))
    pool = Pool(threadCount)
    pool.map(scrap_year, years)
