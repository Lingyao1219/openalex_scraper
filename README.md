# OpenAlex Paper Scraper

This repository contains a Python script to fetch and process academic papers using the [OpenAlex API](https://openalex.org/). The script takes a set of search terms and date ranges, retrieves papers related to those terms, and saves the results as CSV files. 

## Features

- Fetch academic papers based on search terms in titles and abstracts.
- Filter papers by a date range (start year and end year).
- Save retrieved paper data in batches as CSV files.
- Extract detailed metadata including title, abstract, authors, institutions, keywords, citations, and more.

## Prerequisites

Before you begin, ensure you have met the following requirements:
- Python 3.x
- Required libraries: `requests`, `pandas`, `argparse`

You can install the dependencies using the following command:

```bash
pip install requests pandas argparse

## Running the script

To run the script, you will need a search conditions file in .txt format that specifies the search terms and date range. Here's the format of the search conditions file:

```bash
start_year,2020
end_year,2022
search_terms,climate change

You can run the script by providing the path to the search conditions file using the -f option. Optionally, specify the output folder where the results should be saved with -o.

```bash
python openalex_scrape.py -f search_conditions.txt -o results
