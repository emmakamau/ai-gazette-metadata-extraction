import uuid
import json
import nltk
import csv
import requests
import time
import os
import re
from nltk.tokenize import word_tokenize

nltk.download('punkt')
from dotenv import load_dotenv
from db_ops import DatabaseOperations
from openai import AzureOpenAI

load_dotenv()
db_url = os.getenv('DB_URL')
azure_openai_key = os.getenv('AZURE_OPENAI_KEY')
azure_openai_model = os.getenv('MODEL')
azure_openai_endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')


class AzureReadService:
    def __init__(self, cognitive_services_key, cognitive_services_endpoint):
        self.db = DatabaseOperations(db_url)
        self.cognitive_services_key = cognitive_services_key
        self.cognitive_services_endpoint = cognitive_services_endpoint
        self.azure_openai_key = azure_openai_key
        self.azure_openai_model = azure_openai_model
        self.azure_openai_endpoint = azure_openai_endpoint

    def read_document(self, file_path):
        headers = {
            'Ocp-Apim-Subscription-Key': self.cognitive_services_key,
            'Content-Type': 'application/pdf'
        }
        params = {'readingOrder': 'natural'}
        with open(file_path, 'rb') as f:
            data = f.read()
        response = requests.post(
            self.cognitive_services_endpoint + '/vision/v3.2/read/analyze',
            headers=headers,
            params=params,
            data=data
        )
        response.raise_for_status()
        operation_url = response.headers['Operation-Location']

        file_name = os.path.basename(file_path).rstrip('.pdf')

        # Polling for the result
        analysis = None
        while True:  # Loop indefinitely until we break out when processing is complete or fails
            response = requests.get(operation_url, headers={'Ocp-Apim-Subscription-Key': self.cognitive_services_key})
            response.raise_for_status()  # Check the HTTP status code
            analysis = response.json()

            # Check the analysis status
            if 'status' in analysis:
                if analysis['status'] == 'succeeded':
                    full_text_id = self.db.insert_full_text(uuid.uuid4(), str(response.text), str(file_name))
                    break  # Exit the loop if analysis succeeded
                elif analysis['status'] == 'failed':
                    raise Exception("Read document analysis failed")
                # If status is running or notStarted, continue polling
            time.sleep(1)
        print('Successfully read the document')
        return full_text_id

    def extract_sections(self, full_text_id):
        sections = {}
        current_section = ""
        current_page_number = ""
        full_text = self.db.get_full_text_by_id(full_text_id)
        analysis_result = json.loads(full_text.content)
        for page_num, page in enumerate(analysis_result['analyzeResult']['readResults'], start=1):
            for line in page['lines'][:3]:  # Check the first three lines
                text = line['text']
                if text.isdigit():  # Check if the text is a digit
                    current_page_number = text
                    break  # Stop checking once a page number is found
            for line in page['lines']:
                text = line['text']
                if "GAZETTE NOTICE NO." in text:
                    if current_section:  # Add text before "GAZETTE NOTICE NO." to the current section
                        pre_notice_text = text.split("GAZETTE NOTICE NO.")[0].strip()
                        sections[current_section]["content"] += pre_notice_text + " "
                    notice_title = "GAZETTE NOTICE NO." + text.split("GAZETTE NOTICE NO.")[1].split(":")[0].strip()
                    current_section = notice_title
                    sections[current_section] = {"content": "", "page_number": current_page_number}
                    post_notice_text = text.split("GAZETTE NOTICE NO.")[1].split(":", 1)[
                        1].strip() if ":" in text else ""
                    sections[current_section]["content"] += post_notice_text + " "
                elif current_section:  # Only add text if we are within a section
                    sections[current_section]["content"] += text + " "

        if current_section and "Price: KSh" in sections[current_section]["content"]:
            price_index = sections[current_section]["content"].find("Price: KSh")
            sections[current_section]["content"] = sections[current_section]["content"][:price_index].strip()

        self.save_sections(full_text, full_text_id, sections)
        print('Sections saved successfully')

    def save_sections(self, full_text, full_text_id, sections):
        section_batches = self.batch_sections_by_tokens(sections)

        for batch in section_batches:
            metadata_list = self.get_metadata([
                {'gazette': section_name, 'content': section_details}
                for section_name, section_details in batch.items()
            ])
            if isinstance(metadata_list, str):
                data = json.loads(metadata_list)

                if isinstance(data, list):
                    for item in data:
                        if 'Response' in item and item['Response']:
                            # Handle the response
                            continue
                        else:
                            # Extract each item
                            names = item.get('Names')
                            location = item.get('Location')
                            title_no = item.get('Title No')
                            page_no = item.get('Page No')
                            notice_no = item.get('Notice No')
                            self.db.insert_section_text(
                                full_text_id,
                                "content",
                                full_text.name,
                                page_no,
                                notice_no,
                                [names],
                                [title_no],
                                location)

    @staticmethod
    def batch_sections_by_tokens(sections, max_tokens=2500):
        section_batches = []
        current_batch = {}
        current_tokens = 0

        for section_name, section_details in sections.items():
            # Estimate the number of tokens for the current section
            tokens = word_tokenize(section_name + " " + str(section_details))
            section_tokens = len(tokens)

            if section_tokens > 600:
                print(f"Warning: Section '{section_name}' exceeds the max token limit of 600.Skipping this "
                      f"section.")
                continue

            # Check if adding this section would exceed the max token limit
            if current_tokens + section_tokens > max_tokens:
                # If it would, start a new batch
                section_batches.append(current_batch)
                current_batch = {section_name: section_details}
                current_tokens = section_tokens
            else:
                # Add the section to the current batch
                current_batch[section_name] = section_details
                current_tokens += section_tokens

        # Add the last batch if it's not empty
        if current_batch:
            section_batches.append(current_batch)

        return section_batches

    def get_metadata(self, sections):
        client = AzureOpenAI(
            api_key=self.azure_openai_key,
            api_version="2023-08-01-preview",
            azure_endpoint=self.azure_openai_endpoint
        )

        messages = [{"role": "system", "content": self.system_prompt()}]
        combined_content = "\n\n".join([str(section) for section in sections])
        messages.append({"role": "user", "content": combined_content})

        tokens = word_tokenize(self.system_prompt() + " " + str(combined_content))
        total_tokens = len(tokens)
        print(total_tokens)

        response = client.chat.completions.create(
            model=self.azure_openai_model,
            messages=messages,
            temperature=0,
            max_tokens=12000,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )

        return response.choices[0].message.content

    @staticmethod
    def system_prompt():
        prompt = """
            You are a bot designed to extract land related structured data from gazette notices that will be provided to
             as a dictionary with several dictionaries. You are only interested in the following information:
                1. Names: The full names of landowners, comma-separated, with no additional information or numbers.
                2. Location: Specify the location, including 'district of' if mentioned. If the location is not 
                explicitly stated, use the location provided at the footer after "Land Registrar".
                3. Title No.: Any referenced number representing the land, such as Title No., LR No., Parcel No., 
                IR No. or CR No.. Maintain all original special characters and comma-separate multiple title numbers.
                4. Notice No.: The numeric part only of the "GAZETTE NOTICE No." format.
                5. Page No.: Only the page number digit, as defined by "page_number".
        
            Below is a sample input of gazette notices formatted as a dictionary with nested dictionaries. Use this 
            structure to identify and extract the required information.
            {
            'GAZETTE NOTICE NO. 43': {
                'content': 'THE REGISTERED LAND ACT (Cap. 300, section 35) ISSUE OF A NEW LAND TITLE DEED WHEREAS 
                Syprose Helida Odero, of P.O. Box 1089, Kisumu in the Republic of Kenya, is registered proprietor in 
                absolute ownership interest of that piece of land registered under title No. Kisumu/Ojolla/4186, and 
                whereas sufficient evidence has been adduced to show that the land title deed issued thereof has been 
                lost. notice is given that after the expiration of sixty (60) days from the date hereof, I shall issue-a
                new land title deed provided that no objection has been received within that period. Dated the 11th 
                January, 2008 WILLIAM ODHIAMBO, Registrar of Titles, Nairobi. ', 
                'page_number': '14'
                }, 
            'GAZETTE NOTICE NO. 44': {
                'content': 'THE REGISTERED LAND ACT (Cap. 300, section 35) ISSUE OF A NEW LAND TITLE DEED WHEREAS David 
                Ndungu Muchiiri (ID/0564272), of P.O. Box 634, Uthiru in the Republic of Kenya, is registered proprietor
                in absolute ownership interest of that piece of land containing 2.002 hectares or thereabout, situate 
                in the district of Nakuru, registered under title No. Gilgil/Gilgil Block 1/500, and whereas sufficient
                evidence has been adduced to show that the land title deed issued thereof has been lost, notice is 
                given that after the expiration of sixty (60) days from the date hereof, I shall issue a new land 
                title deed provided that no objection has been received within that period. Dated the 11th January, 
                2008. S. W. MUCHEMI, Land Registrar, Nakuru District. ', 
                'page_number': '14'
                },
            'GAZETTE NOTICE NO. 49': {
                'content': "THE REGISTERED LAND ACT (Cap. 300, section 35) ISSUE OF A NEW LAND TITLE DEED WHEREAS 
                Charles Gitonga Kariuki (ID/2714103), of P.O. Box 401, Kisii in the Republic of Kenya, is registered 
                proprietor in absolute ownership interest of that piece of land containing 0.089 hectare or thereabouts,
                situate in the district of Uasin Gishu, registered under title No. Eldoret Mun/Block 21 (King'ong'o)1544,
                and whereas sufficient evidence has been adduced to show that the land title deed issued thereof has 
                been lost, notice is given that after the expiration of sixty (60) days from the date hereof, I shall
                issue a new land title deed provided that no objection has been received within that period. Dated 
                the 1 1th January, 2008. T. M. CHEPKWESI, Land Registrar, Eldoret. GAZETTE NOTICE No. 50 THE 
                REGISTERED LAND ACT (Cap. 300, section 35) ISSUE OF A NEW LAND TITLE DEED WHEREAS (1) Ibrahim Njama
                Mbugua (ID/9778829) and (2) Anthony Mogusi Nyakoni (ID/1590965), both of P.O. Box 8313, Eldoret in 
                the Republic of Kenya, are registered proprietors in absolute ownership interest of that piece of 
                land containing 0.20 hectare or thereabouts, situate in the district of Uasin Gishu, registered under
                title No. Uasin Gishu/Kimumu/1010, and whereas sufficient evidence has been adduced to show that the
                land title deed issued thereof has been lost, notice is given that after the expiration of sixty 
                (60) days from the date hereof, I shall issue a new land title deed provided that no objection has
                been received within that period. Dated the 1 1th January, 2008. T. M. CHEPKWESI, Land Registrar,
                Eldoret, 1 1th January, 2008 THE KENYA GAZETTE 15 ", 
                'page_number': '14'
                }
            }
            
            Below is the expected Response. The format MUST be maintained.
            [
                {
                    "Names": "Syprose Helida Odero",
                    "Location": "Kisumu",
                    "Title No": "Kisumu/Ojolla/4186",
                    "Notice No": 43,
                    "Page No": 14
                },
                {
                    "Names": "David Ndungu Muchiiri",
                    "Location": "district of Nakuru",
                    "Title No": "Gilgil/Gilgil Block 1/500",
                    "Notice No": 44,
                    "Page No": 14
                },
                {
                    "Names": "Charles Gitonga Kariuki",
                    "Location": "district of Uasin Gishu",
                    "Title No": "Eldoret Mun/Block 21 (King'ong'o)1544",
                    "Notice No": 49,
                    "Page No": 14
                },
                {
                    "Names": "Ibrahim Njama Mbugua, Anthony Mogusi Nyakoni",
                    "Location": "district of Uasin Gishu",
                    "Title No": "Uasin Gishu/Kimumu/1010",
                    "Notice No": 50,
                    "Page No": 14
                }
            ]
            
            The response MUST always be in the JSON format provided. 
            
            Keep in mind:
            1. Some of the notices provided might be combined due to poor data extraction e.g. the third 
            dictionary in the user message. Ensure they are separated as highlighted in the expected response. 
            2. page_number might be duplicated or incorrectly read during OCR. They need correction using the provided examples:
                'page_number': '518518217' => "Page No": 518
                'page_number': '516516217' => "Page No": 516
                'page_number': '522522217' => "Page No": 522
                'page_number': '19141914' => "Page No": 1914
                'page_number': '19041904' => "Page No": 1904
                
            3. At this point in time all the gazette notices provided are not necessarily land related. If the notice is 
            not land related the response MUST be in the below format:
             
            [
                {
                    "Response": "None"
                }
            ]
            
            Non-land related notices examples:
            1. "IN THE HIGH COURT OF KENYA AT ELDORET PROBATE AND ADMINISTRATION TAKE NOTICE that an application having 
            been made in this court in: CAUSE NO. 302 OF 2001 By (1) Janifer Muthoni Njenga and (2) Njambi Njenga, both 
            of P.O. Box 1903, Eldoret in Kenya, the deceased's widow and sister-in- law, respectively, for a grant of 
            letters of administration intestate to the estate of Alfred Kihanya Kuria alias Kihanya Kuria, who died at 
            Moi Referral Hospital in Kenya, on 4th May. 2001. The court will proceed to issue the same unless cause be 
            shown to the contrary and appearance in this respect entered within thirty (30) days from the date of the 
            publication of this notice in the Kenya Gazette. Dated the 5th November, 2007. A. B. MONG'ARE, Deputy 
            Registrar, Eldoret. "
            
            2. "IN THE RESIDENT MAGISTRATE'S COURT AT SIRISIA IN THE MATTER OF THE ESTATE OF ELIJAH MUSIOLE WEKUNDU 
            PROBATE AND ADMINISTRATION SUCCESSION CAUSE NO. 7 OF 2007 LET ALL the parties concerned take notice that a 
            petition for a grant of letters of administration intestate to the estate of the above- named deceased, who 
            died at Marinda Village, Kibingei Location, Bungoma North District, on 17th November, 1991, has been filed 
            in this registry by Nathan Munialo Musiole, of P.O. Box 483, Kimilili, in his capacity as an administrator 
            of the deceased's estate. And further take notice that objections in the prescribed form to the making of 
            the proposed grant are invited and must be lodged in this registry within thirty (30) days of publication 
            of this notice. And further take notice that if no objection has been lodged in this registry in the 
            prescribed form within thirty (30) days of the date of publication of this notice, the court may proceed to 
            make the grant as prayed or to make such order as it thinks fit. Dated the 18th December, 2007. R. O. 
            OIGARA, District Registrar, Sirisia."
            
            3. "THE PHYSICAL AND LAND USE PLANNING ACT (No. 13 of 2019) COUNTY GOVERNMENT OF MARSABIT COMPLETION OF PART
             DEVELOPMENT PLAN FOR PRIVATE LAND IN MOYALE SUB-COUNTY AND SAKU SUB-COUNTY OF MARSABIT COUNTYRef. Nos: 
             MYL/194/2023/03, MYL/194/2023/04, MYL/194/2023/05, MYL/194/2023/06, MYL/194/2023/07, MYL/194/2022/06 
             and MBT/276/2022/08. NOTICE is given that the above mentioned part development plans have been completed. 
             The development plans relate to land situated in Sololo, Sessi and Manyatta Area of Moyale Sub-county and 
             Wabera Area of Saku Sub-county, Marsabit County.Copies of the part development plans have been deposited 
             for public inspection at the County Physical Planning Office. The copies so deposited are available for
             inspection free of charge by all persons interested at County Physical Planning Office, between the hours of 
             8.00 a.m. to 5.00 p.m. Monday to Friday.

            Responses MUST be consistent as they will not be rechecked by a human.
            """
        return prompt

    @staticmethod
    def preprocess_name(name):
        try:
            name = str(name)
            # Convert to lowercase and remove extra whitespaces
            name = re.sub(r'\s+', '', name.lower().strip())

            # Handle names separated by comma
            if ',' in name:
                names = name.split(',')
                names = [re.sub(r'[^a-zA-Z0-9\s/]', '', n) for n in names]
                return ','.join(names)

            # Remove special characters
            name = re.sub(r'[^a-zA-Z0-9\s/]', '', name)
            return name
        except:
            return name

    def export_sections_to_csv(self, doc_ids):
        # Open a new CSV file to write to
        home = os.path.expanduser("~")
        download_path = os.path.join(home, 'Downloads', 'sections_export.csv')
        sample_submission_path = "sample_submission.csv"

        with open(sample_submission_path, mode='r', newline='', encoding='utf-8') as sample_file:
            sample_reader = csv.reader(sample_file)
            next(sample_reader)  # Skip the header
            sample_ids = {rows[0] for rows in sample_reader}  # Store in a set for O(1) look-ups

        with open(download_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            # Write the header row
            writer.writerow(['id', 'pred'])
            exported_ids = set()

            # Loop through each full_text_id and process its sections
            for doc_id in doc_ids:
                # Query the database for all sections with the current full_text_id
                sections = self.db.get_sections_by_doc_id(doc_id)

                for section in sections:
                    filename = section.filename
                    if '2022_VOL252' in filename:
                        filename = filename.replace('VOL252', '252')

                    if section.name_of_holder:
                        holder_names = ', '.join(section.name_of_holder).encode('utf-8').decode('utf-8')
                        id_holder = f"{filename}_{section.gazette_notice_number}_name of the holder"
                        writer.writerow([id_holder, self.preprocess_name(holder_names)])

                    if section.registration_number:
                        reg_numbers = ', '.join(section.registration_number).encode('utf-8').decode('utf-8')
                        id_reg = f"{filename}_{section.gazette_notice_number}_Registration numbers"
                        writer.writerow([id_reg, self.preprocess_name(reg_numbers)])

                    if section.location:
                        location = ', '.join(section.location).encode('utf-8').decode('utf-8') if isinstance(section.location, (list, tuple)) else section.location
                        id_location = f"{filename}_{section.gazette_notice_number}_Land location"
                        writer.writerow([id_location, self.preprocess_name(location)])

                    if section.name_of_holder:
                        exported_ids.add(f"{filename}_{section.gazette_notice_number}_name of the holder")
                    if section.registration_number:
                        exported_ids.add(f"{filename}_{section.gazette_notice_number}_Registration numbers")
                    if section.location:
                        exported_ids.add(f"{filename}_{section.gazette_notice_number}_Land location")

            for sample_id in sample_ids:
                if sample_id not in exported_ids:
                    writer.writerow([sample_id, 'none'])


