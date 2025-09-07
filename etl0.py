import pandas as pd 
import glob
import xml.etree.ElementTree as ET 
from datetime import datetime
log_file = "log_file.txt"
target_file = "transformed_data.csv"
def extract_from_csv(f):
    df = pd.read_csv(f)
    return df
def extract_from_json(f):
    df = pd.read_json(f,lines= True)
    return df

def extract_from_xml(f):
    df = pd.DataFrame(columns=["name","height","weight"])
    tree = ET.parse(f) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        df = pd.concat([df, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True) 
    return df
def extract():
    extracted = pd.DataFrame(columns = ["name","height","weight"])
    for file in glob.glob("*.csv") :
        if file != target_file:    
            extracted= pd.concat([extracted ,pd.DataFrame(extract_from_csv(file))],ignore_index = True)
    for file in glob.glob("*.json"):
        extracted = pd.concat([extracted,pd.DataFrame(extract_from_json(file)) ],ignore_index = True)
    for file in glob.glob("*.xml"):
        extracted = pd.concat([extracted,pd.DataFrame(extract_from_xml(file))],ignore_index = True)
    return extracted

def transform(data):
    data['height'] = round(data.height * 0.0254 , 2)
    data['weight'] = round(data.weight *0.45359237,2)
    return data
def load(target_file, transformed):
    transformed.to_csv(target_file)
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now()
    timestamp =now.strftime(timestamp_format)
    with open(log_file , "a") as f :
        f.write(timestamp + ',' + message +'\n')
# Log the initialization of the ETL process 
log_progress("ETL Job Started") 

# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 