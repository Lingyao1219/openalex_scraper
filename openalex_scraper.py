import os
import json
import time
import random
import requests
import pandas as pd
import argparse
from enum import Enum

class FetchMode(Enum):
    ALL = "all"
    RANDOM = "random"

    @classmethod
    def from_string(cls, mode_str):
        try:
            return cls(mode_str.lower())
        except ValueError:
            raise ValueError(f"Invalid mode: {mode_str}. Must be either 'all' or 'random'")


def fetch_papers_with_mode(mode: FetchMode, query, max_papers=None, start_year=None, 
                         end_year=None, save_folder=None, percentage=1):
    """
    Fetch papers based on the specified mode.
    
    Args:
    mode (FetchMode): The mode to fetch papers (ALL or RANDOM)
    query (str): The search query for paper titles and abstracts
    max_papers (int): Maximum number of papers to fetch (required for RANDOM mode)
    start_year (str): Start year for the publication date range
    end_year (str): End year for the publication date range
    save_folder (str): Folder path to save intermediate CSV files
    percentage (float): Percentage of results to save in each batch (for ALL mode)
    
    Returns:
    list: List of dictionaries containing paper data
    """
    if mode == FetchMode.RANDOM:
        if not max_papers:
            raise ValueError("max_papers must be specified when using RANDOM mode")
        return fetch_random_papers(query, max_papers, start_year, end_year, save_folder)
    else:
        return fetch_papers(query, start_year, end_year, save_folder, percentage)


def fetch_random_papers(query, max_papers, start_year=None, end_year=None, save_folder=None):
    """
    Fetch a random sample of papers from the OpenAlex API based on the given query and date range.
    
    Args:
    query (str): The search query for paper titles and abstracts.
    max_papers (int): The maximum number of papers to fetch.
    start_year (str): The start year for the publication date range (optional).
    end_year (str): The end year for the publication date range (optional).
    save_folder (str): The folder path to save intermediate CSV files (optional).
    
    Returns:
    list: A list of dictionaries containing the saved paper data.
    """
    base_url = "https://api.openalex.org/works"
    all_results = []
    per_page = 200  # Maximum allowed by the API
    file_counter = 1
    
    params = {
        "filter": f"title_and_abstract.search:{query}",
        "per-page": per_page,
        "sample": per_page
    }

    if start_year and end_year:
        params["filter"] += f",publication_year:{start_year}-{end_year}"

    batch_results = []
    used_seeds = set()

    while len(all_results) < max_papers:
        seed = random.randint(0, 99999)
        if seed in used_seeds:
            continue
        
        used_seeds.add(seed)
        params["seed"] = seed
        print(f"Fetching {per_page} random samples with seed {seed}...")
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            
            if not results:
                print("No more results to fetch.")
                break

            all_results.extend(results)
            batch_results.extend(results)
            print(f"Total results fetched so far: {len(all_results)}")

            if len(all_results) >= max_papers:
                all_results = all_results[:max_papers]
                print(f"Reached the specified max_papers limit ({max_papers}). Stopping.")
                break

            if len(batch_results) >= 10000:
                save_results(batch_results, save_folder, file_counter)
                file_counter += 1
                batch_results = []

            remaining = max_papers - len(all_results)
            params["sample"] = min(per_page, remaining)
            
        except requests.exceptions.RequestException as e:
            print(f"Error during API request: {e}")
            time.sleep(1)  # Wait longer on error
            continue
            
        time.sleep(0.05)

    if batch_results:
        save_results(batch_results, save_folder, file_counter)
    
    print(f"Successfully saved {len(all_results)} results in total")
    return all_results


def fetch_papers(query, start_year=None, end_year=None, save_folder=None, percentage=1):
    """
    Fetch papers from the OpenAlex API based on the given query and date range.
    
    Args:
    query (str): The search query for paper titles and abstracts.
    start_year (str): The start year for the publication date range (optional).
    end_year (str): The end year for the publication date range (optional).
    save_folder (str): The folder path to save intermediate CSV files (optional).
    percentage (float): Percentage of results to save in each batch.
    
    Returns:
    list: A list of dictionaries containing the saved paper data.
    """
    base_url = "https://api.openalex.org/works"
    all_results = []
    saved_results = []
    per_page = 200  # Maximum allowed by the API
    params = {
        "filter": f"title_and_abstract.search:{query}",
        "per-page": per_page,
    }
    
    if start_year and end_year:
        params["filter"] += f",publication_year:{start_year}-{end_year}"

    cursor = "*"
    file_counter = 1
    batch_results = []

    while cursor:
        params["cursor"] = cursor
        print(f"Fetching results with cursor: {cursor}")
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            
            if not results:
                print("No more results to fetch.")
                break

            all_results.extend(results)
            batch_results.extend(results)
            max_results = data['meta']['count']
            
            print(f"Total results fetched so far: {len(all_results)}")
            print(f"Total results saved so far: {len(saved_results)}")
            print(f"Results in this batch: {len(results)}")
            print(f"Total count according to API: {max_results}")
            
            if len(batch_results) >= 10000:
                results_to_save = random.sample(batch_results, 
                                             int(len(batch_results) * percentage))
                saved_results.extend(results_to_save)
                save_results(results_to_save, save_folder, file_counter)
                print(f"Saved {percentage * 100}% of {len(batch_results)} results")
                file_counter += 1
                batch_results = []

            cursor = data['meta'].get('next_cursor')
            
        except requests.exceptions.RequestException as e:
            print(f"Error during API request: {e}")
            time.sleep(1)  # Wait longer on error
            continue
            
        time.sleep(0.05)

    # Save any remaining results
    if batch_results:
        results_to_save = random.sample(batch_results, 
                                     int(len(batch_results) * percentage))
        saved_results.extend(results_to_save)
        save_results(results_to_save, save_folder, file_counter)
        print(f"Saved final batch of approximately {percentage * 100}% of {len(batch_results)} results")

    print(f"Successfully saved {len(saved_results)} results in total")
    return saved_results


def save_results(results, save_folder, file_counter):
    """
    Save results to a CSV file.
    
    Args:
    results (list): List of paper data to save.
    save_folder (str): Folder path to save the CSV file.
    file_counter (int): Counter for naming the file.
    """
    if save_folder:
        df = create_dataframe(results)
        os.makedirs(save_folder, exist_ok=True)
        file_path = os.path.join(save_folder, f'papers_batch_{file_counter}.csv')
        df.to_csv(file_path, index=False)
        print(f"Saved batch {file_counter} to {file_path}")


def safe_get(d, *keys):
    """
    Safely get a value from a nested dictionary.
    
    Args:
    d (dict): The dictionary to search.
    *keys: The keys to traverse in the dictionary.
    
    Returns:
    The value if found, None otherwise.
    """
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key, {})
        else:
            return None
    return d if d != {} else None


def extract_abstract(abstract_inverted_index):
    """
    Reconstruct the abstract from the abstract_inverted_index.
    
    Args:
    abstract_inverted_index (dict): A dictionary where keys are words and values are lists of positions.
    
    Returns:
    str: Reconstructed abstract as a string.
    """
    if not abstract_inverted_index:
        return ""

    max_position = max(pos for positions in abstract_inverted_index.values() 
                      for pos in positions)
    abstract = [""] * (max_position + 1)

    for word, positions in abstract_inverted_index.items():
        for pos in positions:
            abstract[pos] = word
    return " ".join(abstract)


def extract_author_info(auth):
    """
    Extract author information from an authorship dictionary.
    
    Args:
    auth (dict): Authorship dictionary.
    
    Returns:
    dict: Extracted author information.
    """
    author = auth.get('author', {})
    institution = auth.get('institutions', [{}])[0] if auth.get('institutions') else {}
    
    return {
        'author_name': author.get('display_name'),
        'institution': institution.get('display_name'),
        'institution_type': institution.get('type'),
        'country_code': institution.get('country_code'),
        'corresponding': auth.get('is_corresponding', False),
        'affiliation_strings': auth.get('raw_affiliation_strings'),
    }


def extract_paper_info(paper):
    """
    Extract relevant data from a single paper dictionary.
    
    Args:
    paper (dict): Dictionary containing paper data.
    
    Returns:
    dict: Extracted and processed paper data.
    """
    abstract = extract_abstract(safe_get(paper, 'abstract_inverted_index'))
    authorships = safe_get(paper, 'authorships') or []
    keywords = safe_get(paper, 'keywords') or []
    concepts = safe_get(paper, 'concepts') or []
    topics = safe_get(paper, 'topics') or []

    return {
        'id': safe_get(paper, 'id'),
        'doi': safe_get(paper, 'doi'),
        'title': safe_get(paper, 'title'),
        'abstract': abstract,
        'publication_year': safe_get(paper, 'publication_year'),
        'publication_date': safe_get(paper, 'publication_date'),
        'created_date': safe_get(paper, 'created_date'),
        'type': safe_get(paper, 'type'),
        'cited_by_count': safe_get(paper, 'cited_by_count'),
        'is_retracted': safe_get(paper, 'is_retracted'),
        'is_paratext': safe_get(paper, 'is_paratext'),
        'journal': safe_get(paper, 'primary_location', 'source', 'display_name'),
        'author_count': len(authorships),
        'open_access': safe_get(paper, 'open_access', 'is_oa'),
        'oa_status': safe_get(paper, 'open_access', 'oa_status'),
        'affiliations': [extract_author_info(auth) for auth in authorships],
        'concepts': '; '.join([concept.get('display_name', '') for concept in concepts]),
        'keywords': '; '.join([kw.get('display_name', '') for kw in keywords]),
        'topics': '; '.join([safe_get(topic, 'display_name') or '' for topic in topics]),
        'subfields': '; '.join([safe_get(topic, 'subfield', 'display_name') or '' for topic in topics]),
        'fields': '; '.join([safe_get(topic, 'field', 'display_name') or '' for topic in topics]),
        'domains': '; '.join([safe_get(topic, 'domain', 'display_name') or '' for topic in topics]),
        'referenced_works_count': len(safe_get(paper, 'referenced_works') or []),
        'referenced_works': '; '.join([work or '' for work in safe_get(paper, 'referenced_works') or []]),
        'funding_details': '; '.join([safe_get(f, 'display_name') or '' for f in safe_get(paper, 'funding') or []]),
        'license': safe_get(paper, 'open_access', 'license'),
        'metrics': safe_get(paper, 'counts_by_year') or {},
        'host_venue_issn': safe_get(paper, 'primary_location', 'source', 'issn'),
        'publisher': safe_get(paper, 'primary_location', 'source', 'publisher'),
        'relevance_score': safe_get(paper, 'relevance_score'),
        'language': safe_get(paper, 'language'),
        'host_organization_name': safe_get(paper, 'primary_location', 'source', 'host_organization_name'),
        'is_accepted': safe_get(paper, 'primary_location', 'is_accepted'),
        'type_crossref': safe_get(paper, 'type_crossref'),
        'indexed_in': safe_get(paper, 'indexed_in'),
    }


def create_dataframe(papers):
    """
    Create a detailed DataFrame from the list of paper dictionaries.
    
    Args:
    papers (list): List of dictionaries containing paper data.
    
    Returns:
    pandas.DataFrame: A DataFrame with detailed information about the papers.
    """
    data = []
    for paper in papers:
        if not isinstance(paper, dict):
            continue

        paper_data = extract_paper_info(paper)
        data.append(paper_data)
    
    return pd.DataFrame(data)


def process_search_file(file_path):
    """
    Read and process the search conditions file.
    
    Args:
    file_path (str): Path to the search conditions file.
    
    Returns:
    tuple: (start_year, end_year, query)
    """
    start_year, end_year, query = None, None, None
    with open(file_path) as f:
        for line in f:
            key, value = line.strip().split(',')
            if key == "start_year":
                start_year = value
            elif key == "end_year":
                end_year = value
            elif key == "search_terms":
                query = value
    return start_year, end_year, query


def save_dataset(df, save_folder, filename='complete_dataset.csv'):
    """
    Save a DataFrame to a CSV file.
    
    Args:
    df (pandas.DataFrame): The DataFrame to save.
    save_folder (str): The folder path to save the CSV file.
    filename (str): The name of the CSV file (default: 'complete_dataset.csv').
    """
    os.makedirs(save_folder, exist_ok=True)
    file_path = os.path.join(save_folder, filename)
    df.to_csv(file_path, index=False)
    print(f"Saved dataset to {file_path}")


def main():
    """
    Main function to run the paper fetching and processing script.
    """
    parser = argparse.ArgumentParser(description="Fetch and process academic papers based on search conditions.")
    parser.add_argument("-m", "--mode", required=True, choices=['all', 'random'],
                       help="Mode to fetch papers: 'all' for complete dataset, 'random' for random sampling")
    parser.add_argument("-f", "--file", required=True,
                       help="Path to the search conditions file")
    parser.add_argument("-o", "--output", default=None,
                       help="Output folder path (default: same name as input file)")
    parser.add_argument("-p", "--percentage", type=float, default=1.0,
                       help="Percentage of results to save in each batch (default: 100, minimal:0.01)")
    parser.add_argument("-n", "--max_papers", type=int, default=None,
                       help="Maximum number of papers to fetch (required for random mode)")

    args = parser.parse_args()

    if args.mode == 'random' and args.max_papers is None:
        parser.error("--max_papers is required when using random mode")
    
    if args.percentage < 0.01 or args.percentage > 100:
        parser.error("Percentage must be between 0.01 and 100")

    # Process input file and set up output directory
    start_year, end_year, query = process_search_file(args.file)
    save_folder = args.output if args.output else os.path.splitext(args.file)[0]

    try:
        # Fetch papers based on mode
        fetch_mode = FetchMode.from_string(args.mode)
        
        papers = fetch_papers_with_mode(
            mode=fetch_mode,
            query=query,
            max_papers=args.max_papers,
            start_year=start_year,
            end_year=end_year,
            save_folder=save_folder,
            percentage=args.percentage / 100
        )

        # Process and save results
        # if papers:
        #     df = create_dataframe(papers)
        #     save_dataset(df, save_folder)
        #     print(f"Successfully processed and saved {len(papers)} papers.")
        # else:
        #     print("No papers were fetched. Please check your search criteria.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    main()
