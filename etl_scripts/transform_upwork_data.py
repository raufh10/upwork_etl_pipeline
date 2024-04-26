import os
import re
import json

# Main function
def main():
    
    print('Transforming Upwork data...')

    # Transform the skill categories data
    transform_skill_categories()
    print('Skill categories transformation complete.')

    # Transform the job data
    transform_job_data()
    print('Job data transformation complete.')

    # Merge the skills and jobs data
    mix_skills_and_jobs()
    print('Merging skills and jobs complete.')

    # Flatten skills
    flatten_skills(r'/home/raufhamidy/Documents/upwork_etl_pipeline/job_data.json')
    print('Skills flattened.')

    # Delete the skill categories file
    delete_skills_categories(r'/home/raufhamidy/Documents/upwork_etl_pipeline/skill_categories.json')
    print('Skill categories file deleted.')

    print('Upwork data transformation complete.')
    
def transform_skill_categories():

    # File path
    file_path = r'/home/raufhamidy/Documents/upwork_etl_pipeline/skill_categories.json'
    
    # Separate title and id
    separate_title_id_skills_categories(file_path)
    # Remove duplicate skills
    check_remove_duplicate(file_path)
    # Clean skills
    clean_skills(file_path)

def transform_job_data():

    # File path
    file_path = r'/home/raufhamidy/Documents/upwork_etl_pipeline/job_data.json'

    # Separate title and id
    separate_title_id_job_data(file_path)
    # Remove duplicate jobs
    check_remove_duplicate(file_path)
    # Clean domestic labels
    clean_domestic_labels(file_path)
    # Clean description
    clean_description(file_path)
    # Assign key features
    assign_key_features(file_path)
    # Flatten features
    flatten_features(file_path)

def mix_skills_and_jobs():

    # Merge the skills and jobs data
    with open(r'/home/raufhamidy/Documents/upwork_etl_pipeline/skill_categories.json', 'r', encoding='utf-8') as skills_json, \
         open(r'/home/raufhamidy/Documents/upwork_etl_pipeline/job_data.json', 'r', encoding='utf-8') as jobs_json:
    
        # Load the data
        skills_data = json.load(skills_json)
        jobs_data = json.load(jobs_json)

        job_list = []

        # Iterate over the jobs data
        for job in jobs_data:
            job_dict = {}
            job_id = job['job_id']

            # Add the job details to the dictionary
            job_dict['job_id'] = job_id
            job_dict['title'] = job['title']
            job_dict['job_url'] = job['job_url']
            job_dict['domestic_label'] = job['domestic_label']
            job_dict['description'] = job['description']
            job_dict['project_type'] = job['project_type']
            job_dict['hours_per_week'] = job['hours_per_week']
            job_dict['duration'] = job['duration']
            job_dict['experience_level'] = job['experience_level']
            job_dict['hourly_rate'] = job['hourly_rate']
            job_dict['fixed_price'] = job['fixed_price']
            job_dict['contract_to_hire'] = job['contract_to_hire']
            job_dict['remote_job'] = job['remote_job']

            # Find the skills for the job
            for skill in skills_data:
                if skill['job_id'] == job_id:
                    job_dict['skills'] = skill['skills']

            # Append the job dictionary to the list
            job_list.append(job_dict)

    # Save the merged data to a new file
    with open(r'/home/raufhamidy/Documents/upwork_etl_pipeline/job_data.json', 'w', encoding='utf-8') as json_file:
        json.dump(job_list, json_file, indent=4, ensure_ascii=False)

def flatten_skills(file_path):

    # Load the data from the file
    with open(file_path, 'r', encoding='utf-8') as job_json:
        job_data = json.load(job_json)

    for item in job_data:
        # Convert skills list to string
        try:
            item['skills'] = ', '.join(item['skills'])
        except:
            item['skills'] = 'N/A'

    # Save the flattened data back to the file
    with open(file_path, 'w', encoding='utf-8') as clean_job_json:
        json.dump(job_data, clean_job_json, ensure_ascii=False, indent=4)

def delete_skills_categories(file_path):

    # Use OS to delete the file
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"The file {file_path} has been deleted.")
    else:
        print(f"The file {file_path} does not exist.")

def check_remove_duplicate(file_path):
    
    # Load the data from the file
    with open(file_path, 'r', encoding='utf-8') as job_json:
        job_data = json.load(job_json)

    # Check for duplicate job ids
    job_ids = []
    unique_jobs = []
    duplicate_count = 0

    for item in job_data:
        job_id = item['job_id']
        if job_id not in job_ids:
            unique_jobs.append(item)
            job_ids.append(job_id)
        else:
            print(f'Duplicate job id: {job_id}')
            duplicate_count += 1

    # Save the unique jobs to the file
    with open(file_path, 'w', encoding='utf-8') as clean_job_json:
        json.dump(unique_jobs, clean_job_json, ensure_ascii=False, indent=4)

    # Print the results
    print(f'Total job ids: {len(unique_jobs)} vs {len(job_data)} (Duplicate count: {duplicate_count})')

def separate_title_id_skills_categories(file_path):
    
    # Initialize an empty list to store the transformed data
    skill_categories_dict = []

    # Separate title and id
    with open(file_path, 'r', encoding='utf-8') as skills_json:
        skills_data = json.load(skills_json)
        for item in skills_data:
            job_dict = {}

            job_code = item['job_id']
            pattern = r'-(?!.*-)'

            parts = re.split(pattern, job_code)
            title, job_id = parts[0], parts[1]

            skills = item['skills']

            job_dict['job_id'] = int(job_id)
            job_dict['title'] = title
            job_dict['skills'] = skills

            skill_categories_dict.append(job_dict)

    # Save the transformed data back to the file
    with open(file_path, 'w', encoding='utf-8') as json_file:
        json.dump(skill_categories_dict, json_file, indent=4)

def clean_skills(file_path):
    
    # Load the data from the file
    with open(file_path, 'r', encoding='utf-8') as job_json:
        job_data = json.load(job_json)

    # Clean the skills from newlines
    for item in job_data:
        skills = item['skills']

        # Collect items to remove in a separate list
        items_to_remove = [i for i in skills if '\n' in i]

        # Remove the items
        for i in items_to_remove:
            skills.remove(i)

        item['skills'] = list(set(skills))

    # Save the cleaned data back to the file
    with open(file_path, 'w', encoding='utf-8') as clean_job_json:
        json.dump(job_data, clean_job_json, ensure_ascii=False, indent=4)

def separate_title_id_job_data(file_path):
    
    # Initialize an empty list to store the transformed data
    job_data_dict = []

    # Separate title and id
    with open(file_path, 'r', encoding='utf-8') as jobs_json:
        jobs_data = json.load(jobs_json)
        for item in jobs_data:
            job_dict = {}

            job_code = item['job_id']
            pattern = r'-(?!.*-)'

            parts = re.split(pattern, job_code)
            title, job_id = parts[0], parts[1]

            job_url = item['job_url']

            try:
                domestic_label = item['domestic_label']
            except:
                domestic_label = 'N/A'

            try:
                description = item['description']
            except:
                description = 'N/A'

            try:
                features = item['features']
            except:
                features = 'N/A'

            job_dict['job_id'] = int(job_id)
            job_dict['title'] = title
            job_dict['job_url'] = job_url
            job_dict['domestic_label'] = domestic_label
            job_dict['description'] = description
            job_dict['features'] = features

            job_data_dict.append(job_dict)

    # Save the transformed data back to the file
    with open(file_path, 'w', encoding='utf-8') as json_file:
        json.dump(job_data_dict, json_file, indent=4)

def clean_domestic_labels(file_path):

    # Load the data from the file
    with open(file_path, 'r', encoding='utf-8') as job_json:
        job_data = json.load(job_json)

    # Clean the domestic labels
    for item in job_data:
        try:
            domestic_label = item['domestic_label']

            if domestic_label == 'Only freelancers located in the U.S. may apply. U.S. located freelancers only':
                domestic_label = 'U.S. only'

            elif domestic_label == 'Browse Data Analytics Jobs to find jobs just like this one!':
                domestic_label = 'N/A'

            elif domestic_label == 'Browse Data Visualization Jobs to find jobs just like this one!':
                domestic_label = 'N/A'

        except:
            domestic_label = 'N/A'

        item['domestic_label'] = domestic_label

    # Save the cleaned data back to the file
    with open(file_path, 'w', encoding='utf-8') as clean_job_json:
        json.dump(job_data, clean_job_json, ensure_ascii=False, indent=4)

def clean_description(file_path):

    # Load the data from the file
    with open(file_path, 'r', encoding='utf-8') as job_json:
        job_data = json.load(job_json)

    # Clean the description
    for item in job_data:

        description = item['description']

        # Remove newlines
        description = description.replace('\n', ' ')

        # Remove extra spaces
        description = ' '.join(description.split())

        item['description'] = description

    # Save the cleaned data back to the file
    with open(file_path, 'w', encoding='utf-8') as clean_job_json:
        json.dump(job_data, clean_job_json, ensure_ascii=False, indent=4)

def assign_key_features(file_path):

    # Load the data from the file
    with open(file_path, 'r', encoding='utf-8') as job_json:
        job_data = json.load(job_json)

    # Assign key features
    for item in job_data:
        
        features = item['features']

        features_dict = {}

        for feature in features:
            # Get project type
            if 'Project Type' in feature:
                project_type = feature.replace(' Project Type', '')
                features_dict['project_type'] = project_type
            # Get hours per week
            if 'hrs/week Hourly' in feature:
                hours_per_week = feature.replace(' hrs/week Hourly', '')
                features_dict['hours_per_week'] = hours_per_week
            # Get duration
            if 'Duration' in feature:
                duration = feature.replace(' Duration', '')
                features_dict['duration'] = duration
            # Get experience level
            if 'Experience Level' in feature:
                experience_level = feature.replace(' Experience Level', '')
                features_dict['experience_level'] = experience_level
            # Get hourly rate
            if 'Hourly' in feature:
                hourly_rate = feature.replace(' Hourly', '')
                features_dict['hourly_rate'] = hourly_rate
            # Get fixed price
            if 'Fixed-price' in feature:
                fixed_price = feature.replace(' Fixed-price', '')
                features_dict['fixed_price'] = fixed_price
            # Get contract-to-hire or not
            if 'Contract-to-hire' in feature:
                contract_to_hire = True
            else:
                contract_to_hire = False
            features_dict['contract_to_hire'] = contract_to_hire
            # Get remote job or not
            if 'Remote Job' in feature:
                remote_job = True
            else:
                remote_job = False
            features_dict['remote_job'] = remote_job

        item['features'] = features_dict

    # Save the cleaned data back to the file
    with open(file_path, 'w', encoding='utf-8') as clean_job_json:
        json.dump(job_data, clean_job_json, ensure_ascii=False, indent=4)

def flatten_features(file_path):

    # Load the data from the file
    with open(file_path, 'r', encoding='utf-8') as job_json:
        job_data = json.load(job_json)

    # List of all possible features
    possible_features = [
        "hours_per_week",
        "hourly_rate",
        "fixed_price",
        "contract_to_hire",
        "remote_job",
        "duration",
        "experience_level",
        "project_type"
    ]
    
    # Process each job in the data
    for job in job_data:
        # Initialize a dictionary to store the flattened features
        flattened_features = {}
        
        # Check if the job has features
        if "features" in job and isinstance(job["features"], dict):
            # For each possible feature, add it to the flattened features
            for feature in possible_features:
                if feature in job["features"]:
                    flattened_features[feature] = job["features"][feature]
                else:
                    flattened_features[feature] = "N/A"
        else:
            # If no features are present, add all with "N/A"
            for feature in possible_features:
                flattened_features[feature] = "N/A"
        
        # Update the job with the flattened features
        job.update(flattened_features)
        
        # Remove the original features
        job.pop("features", None)

    # Save the updated data back to the file
    with open(file_path, 'w', encoding='utf-8') as clean_job_json:
        json.dump(job_data, clean_job_json, ensure_ascii=False, indent=4)

if __name__ == '__main__':
    main()
