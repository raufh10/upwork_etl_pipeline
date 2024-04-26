from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import os
import re
import json
import time

# Main Function
def main():

    # File Paths
    job_data_file_path = r'/home/raufhamidy/Documents/upwork_etl_pipeline/job_data.json'
    skill_categories_file_path = r'/home/raufhamidy/Documents/upwork_etl_pipeline/skill_categories.json'
    temp_job_data_file_path = r'/home/raufhamidy/Documents/upwork_etl_pipeline/temp_job_data.json'

    print('Starting Upwork Data Extraction...')

    url = 'https://www.upwork.com/nx/jobs/search/?q=Data%20analyst&sort=recency&category2_uid=531770282580668420&subcategory2_uid=531770282593251330&per_page=50'
    page_max = 5

    print('Extracting job list...')

    extract_job_list(url, page_max, job_data_file_path, skill_categories_file_path)
    print('Job list extracted.')

    print('Extracting job pages...')

    job_urls = []
    load_job_urls(job_urls, job_data_file_path)
    extract_job_page(job_urls, temp_job_data_file_path)
    print('Job pages extracted.')

    print('Loading temp job data...')
    
    load_temp_job_data(job_data_file_path, temp_job_data_file_path)
    print('Temp job data loaded.')
    
    delete_temp_job_data(temp_job_data_file_path)
    print('Temp job data deleted.')

    print('Upwork Data Extraction Complete.')

def loading_page(page, url):

    print('Loading page...')

    # Max attempts
    max_attempts = 3
    # For loop to try loading page
    for i in range(max_attempts):
        print(f'Attempt {i+1}')

        try:
            # Page Load
            page.goto(url, timeout=600000)
            page.wait_for_load_state()
            
            # Check if page is loaded
            if page.wait_for_selector('.nav-logo'):
                for _ in range(20):
                    page.keyboard.press('PageDown')
                    time.sleep(0.5)
                page.keyboard.press('End')
                page.keyboard.press('Home')
                page.wait_for_load_state()
                print('Page loaded')
                break

            # If page is not loaded, try again
            else:
                print('Page not loaded')
                time.sleep(1)
                continue
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(5)
            continue

def turn_json_into_list_of_dicts(file_path):

    print(f'Turning {file_path} into a list of dictionaries...')

    # Turn JSON file into a list of dictionaries
    list_of_dicts = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            dict_data = json.loads(line)
            list_of_dicts.append(dict_data)

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(list_of_dicts, f, indent=4, ensure_ascii=False)

    print(f'{file_path} has been turned into a list of dictionaries.')

def extract_job_list(url, page_max, job_data_file_path, skill_categories_file_path):

    # Playwright sequence
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        starting_url = 'https://www.upwork.com/nx/jobs/search/?nbs=1&q=data%20analyst&per_page=50'
        page.goto(starting_url, timeout=600000)

        # Scraping the job data
        loading_page(page, url)

        jobs_data = []
        for i in range(1, page_max+1):

            print(f'Extracting page {i}...')

            # Parse HTML
            soup = BeautifulSoup(page.content(), "lxml")
            
            try:
                # Get skill categories & return job_data
                extract_job_list_data(soup, jobs_data, job_data_file_path, skill_categories_file_path)
                print('Job data extracted.')
            
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(1)
                continue

            # Load next page
            print(f'Loading page {i+1}...')
            load_next_page(page, i)

        # Turn job_data into a list of dictionaries
        file_path_job_data = job_data_file_path
        turn_json_into_list_of_dicts(file_path_job_data)
        
        # Turn skill_categories into a list of dictionaries
        file_path_skills = skill_categories_file_path
        turn_json_into_list_of_dicts(file_path_skills)
        
        # Close Playwright
        browser.close()

def extract_job_list_data(soup, job_data, job_data_file_path, skill_categories_file_path):

    # Get Skill Categories
    post_container = soup.find('section', {'class':'card-list-container'})
    posts = post_container.findAll('article')

    # Loop through each post
    for post in posts:
        job_data_dict = {}
        skill_categories = {}

        # Get Job Data
        title_link_tag = post.find('h2', {'class':'h5 mb-0 mr-2 job-tile-title'})
        title_url = title_link_tag.text.strip()

        data_ev_job_uid = post['data-ev-job-uid']
        job_id = f'{title_url}-{data_ev_job_uid}'

        url_tag = title_link_tag.find('a')
        url_href = url_tag['href']
        job_url = f'https://www.upwork.com{url_href}'

        # Store Job Data in job_data
        job_data_dict['title'] = title_url
        job_data_dict['job_id'] = job_id
        job_data_dict['job_url'] = job_url
        job_data.append(job_data_dict)

        # Store the data in a JSON file
        with open(job_data_file_path, 'a', encoding='utf-8') as json_file:
            json.dump(job_data_dict, json_file, ensure_ascii=False)
            json_file.write('\n')

        # Get Job Skills List
        skills = []
        try:
            skill_container = post.find('div', {'class':'air3-token-container'})
            skills_tag = skill_container.findAll('span')
            for skill in skills_tag:
                skills.append(skill.text)
        except:
            skills = ['N/A']

        # Store Job Skills List in skill_categories
        skill_categories['job_id'] = job_id
        skill_categories['skills'] = skills

        # Store the data in a JSON file
        with open(skill_categories_file_path, 'a', encoding='utf-8') as json_file:
            json.dump(skill_categories, json_file, ensure_ascii=False)
            json_file.write('\n')

def load_next_page(page, page_index):

    print('Loading page...')

    url = f'https://www.upwork.com/nx/search/jobs/?category2_uid=531770282580668420&page={page_index+1}&per_page=50&q=Data%20analyst&sort=recency&subcategory2_uid=531770282593251330'

    # Max attempts
    max_attempts = 3

    # For loop to try loading page
    for _ in range(max_attempts):

        try:
            # Page Load
            page.goto(url, timeout=600000)
            page.wait_for_load_state()

            # Check if page is loaded
            if page.wait_for_selector('.nav-logo'):
                for _ in range(20):
                    page.keyboard.press('PageDown')
                    time.sleep(0.5)
                page.keyboard.press('End')
                page.keyboard.press('Home')
                page.wait_for_load_state()
                print('Page loaded')
                break

            # If page is not loaded, try again
            else:
                print('Page not loaded')
                time.sleep(1)
                continue

        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(1)
            continue

def load_job_urls(job_urls, job_data_file_path):
     
    print('Loading job URLs...')
     
    with open(job_data_file_path, 'r') as f:
        list_of_dicts = json.load(f)
        for dict in list_of_dicts:
            job_urls.append(dict['job_url'])

def extract_job_page(job_urls, temp_job_data_file_path):
    
    # Playwright sequence
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        # Scraping the job data
        for url in job_urls:

            print(f'Extracting job page {url}...')
            
            # Load Job Page
            loading_page(page, url)
    
            # Get Job Data
            extract_job_data(page, url, temp_job_data_file_path)
    
        # Close Playwright
        browser.close()

def extract_job_data(page, url, temp_job_data_file_path):

    # Initialize job_data_dict
    job_data_dict = {}

    # Parse HTML
    soup = BeautifulSoup(page.content(), "lxml")

    # Get Domestic Label
    try:
        domestic_label = soup.find('span', {'class':'text-light-on-muted'}).text.strip()
    except:
        domestic_label = 'N/A'

    # Get Job Description
    try:
        description = soup.find('section', {'class':'air3-card-section py-4x'}).text.strip()
    except:
        description = 'N/A'

    # Get Job Features
    try:
        features = []
        job_features_tag = soup.find('ul', {'class':'features list-unstyled m-0'})
        job_features = job_features_tag.findAll('li')
        for job_feature in job_features:
            job_feature = job_feature.text.strip()
            job_feature = re.sub(r'\s+', ' ', job_feature)
            features.append(job_feature)
    except:
        features = 'N/A'

    # Store Job Data in job_data_dict
    job_data_dict['job_url'] = url
    job_data_dict['domestic_label'] = domestic_label
    job_data_dict['description'] = description
    job_data_dict['features'] = features

    # Store the data in a JSON file
    with open(temp_job_data_file_path, 'a', encoding='utf-8') as json_file:
        json.dump(job_data_dict, json_file, ensure_ascii=False)
        json_file.write('\n')

def load_temp_job_data(job_data_file_path, temp_job_data_file_path):

    print('Loading temp job data...')

    # Turn temp_job_data into a list of dictionaries
    file_path = job_data_file_path
    temp_file_path = temp_job_data_file_path
    turn_json_into_list_of_dicts(temp_file_path)

    with open(file_path, 'r', encoding='utf-8') as f, open(temp_file_path, 'r', encoding='utf-8') as tf:
        jobs = json.load(f)
        dicts = json.load(tf)
        
        # Convert list_of_dicts to a dictionary for quick lookup
        dicts_lookup = {item['job_url']: item for item in dicts}
        
        # Iterate through each job and update details if present in dicts_lookup
        for job in jobs:
            matching_dict = dicts_lookup.get(job['job_url'])
            if matching_dict:
                # Update only the keys that exist in matching_dict to avoid overwriting with None
                for key in ['domestic_label', 'description', 'features']:
                    if key in matching_dict:
                        job[key] = matching_dict[key]

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, indent=4, ensure_ascii=False)

    print('Temp job data loaded.')

def delete_temp_job_data(temp_job_data_file_path):

    print('Deleting temp job data...')

    # Delete temp_job_data
    file_path = temp_job_data_file_path

    # Check if file exists before trying to delete it
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"The file {file_path} has been deleted.")
    else:
        print(f"The file {file_path} does not exist.")

if __name__ == "__main__":
    main()
    