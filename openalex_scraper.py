import os
import json
import time
import requests
import pandas as pd
import argparse


def fetch_papers(query, start_year=None, end_year=None, save_folder=None):
    """
    Fetch papers from the OpenAlex API based on the given query and date range.
    
    Args:
    query (str): The search query for paper titles and abstracts.
    start_year (str): The start year for the publication date range (optional).
    end_year (str): The end year for the publication date range (optional).
    save_folder (str): The folder path to save intermediate CSV files (optional).
    
    Returns:
    list: A list of dictionaries containing paper data.
    """
    base_url = "https://api.openalex.org/works"
    all_results = []
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
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            print(f"Response content: {response.text}")
            break

        data = response.json()
        results = data.get('results', [])
        
        if not results:
            print("No more results to fetch.")
            break

        #all_results.extend(results)
        batch_results.extend(results)
        max_results = data['meta']['count']
        
        print(f"Total results fetched so far: {len(all_results)}")
        print(f"Results in this batch: {len(results)}")
        print(f"Total count according to API: {max_results}")
        
        # if len(all_results) % 1000 == 0 or len(all_results) >= max_results:
        #     save_results(all_results, save_folder, file_counter)
        #     file_counter += 1

        if len(batch_results) >= 1000 or len(all_results) + len(batch_results) >= max_results:
            all_results.extend(batch_results[:1000])  # Only take up to 1000 results
            save_results(batch_results[:1000], save_folder, file_counter)
            file_counter += 1
            batch_results = batch_results[1000:]  # Keep any excess for the next batch
            print(f"Saved batch of 1000 results")


        if len(all_results) >= max_results:
            all_results = all_results[:max_results]  # Trim to exact number
            print(f"Reached max_results ({max_results}). Stopping.")
            break
        
        cursor = data['meta'].get('next_cursor')
        time.sleep(0.1)

    print(f"Retrieved {len(all_results)} results")
    return all_results


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

    max_position = max(pos for positions in abstract_inverted_index.values() for pos in positions)
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
        'citing_papers_count': len(safe_get(paper, 'referenced_by') or []),
        'citing_papers': '; '.join([citing['id'] for citing in safe_get(paper, 'referenced_by') or []]),
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
    # Set up the argument parser
    parser = argparse.ArgumentParser(description="Fetch and process academic papers based on search conditions.")
    parser.add_argument("-f", "--file", required=True, help="Path to the search conditions file")
    parser.add_argument("-o", "--output", default=None, help="Output folder path (default: same name as input file)")
    args = parser.parse_args()

    start_year, end_year, query = process_search_file(args.file)
    if args.output:
        save_folder = args.output
    else:
        save_folder = os.path.splitext(args.file)[0]

    papers = fetch_papers(query, start_year, end_year, save_folder)
    df = create_dataframe(papers)
    save_dataset(df, save_folder)


if __name__ == "__main__":
    main()