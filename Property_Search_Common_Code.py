
import concurrent.futures
import csv
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import time
import keyboard
import math
import datetime
from datetime import date
import pandas as pd
from dateutil.relativedelta import relativedelta
import os
from selenium.webdriver.chrome.options import Options as ChromeOptions
import base64
import re
import multiprocessing as mp
import threading
import requests

def search_and_rename_folder(folder_path, target_folder_name, new_folder_name):
    for root, dirs, files in os.walk(folder_path):
        for dir_name in dirs:
            if dir_name == target_folder_name:
                old_folder_path = os.path.join(root, dir_name)
                new_folder_path = os.path.join(root, new_folder_name)
                os.rename(old_folder_path, new_folder_path)
            

def check_state_county_available(state_county):
    with open("F:\\Kartheek\\Search-Data\\3 County information-FL.csv") as csv_file:
        county_info = csv.DictReader(csv_file)
        for county_info_row in county_info:
            if county_info_row["State-County"].lower() == state_county.lower():
                return True
    return False

def handle_state_county_unavailable(fullsearchaddress):
    folder_path = "F:\Kartheek\Property-Info"
    target_folder_name = rf'{fullsearchaddress} - Automation Failed'
    new_folder_name = f'{fullsearchaddress}-Automation NA'
    search_and_rename_folder(folder_path, target_folder_name, new_folder_name)
    
# def download_pdf(url, filename):
#     response = requests.get(url, stream=True)
#     with open(filename, 'wb') as file:
#         for chunk in response.iter_content(chunk_size=8192):
#             file.write(chunk)
            
def pdf_viewer_print(path_and_filename,current_url):
    response = requests.get(current_url, stream=True)
    time.sleep(5)
    with open(path_and_filename, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    time.sleep(10)

def non_pdf_viewer_print(result,path_and_filename):
    pdf_content_base64  = result['data']
    pdf_content = base64.b64decode(pdf_content_base64)
    # pyautogui.press('enter')
    

    with open(path_and_filename, "wb") as file:
        file.write(pdf_content)
    time.sleep(10)




streetname0=""
streetname1=""
Current_Street_Type=""
Current_Street_Direction=""
currentstreetnumber=""
currentstreetname = ""
county_row_data = {}

Tax_ID = ""
Owner_Name_1 = ""
Owner_Name_2 = ""



#read address 
# Load the Excel file
df = pd.read_excel('F:\\Kartheek\\Search-Data\\All State Property Address.xlsx')

street_direction = ['north','east','west','south','northwest','northeast','southwest','southeast','n','e','w','s','nw','ne','sw','se']

street_type = ['street','avenue','boulevard','way','lane','drive','terrace','place','court','road','highway','freeway','expressway','interstate','turnpike','beltway','parkway','causeway','st','ave','blvd','way','la','dr','ter','pl','ct','rd','hwy','fwy','expy','i','tpke','bltwy','pkwy','cswy']

# Split the cell values based on commas into new columns
split_data = df['Address'].str.split(',', expand=True)

# Rename the columns in split_data
split_data.columns = ['Address', 'City_Name', 'State_Zip']

# Drop the original column from the DataFrame
df.drop(columns=['Address'], inplace=True)

# Concatenate the original DataFrame with split_data DataFrame
df = pd.concat([df, split_data], axis=1)


#Create an empty data frame
df2 = pd.DataFrame(columns = ['streetnumber', 'direction', 'streetname', 'streettype'])


for currentaddress in df['Address']:
    currentaddress_list = currentaddress.split(" ")
    currentaddress_lower_list = [item.lower() for item in currentaddress_list]
    
    if currentaddress_lower_list[1] not in street_direction and currentaddress_lower_list[1] not in street_type:
        streetname0 = currentaddress_lower_list[1]
    if currentaddress_lower_list[2] not in street_direction and currentaddress_lower_list[2] not in street_type:
        streetname1 = currentaddress_lower_list[2]
    
    if streetname1 != "":
        streetname = streetname0+" "+streetname1
    else:
        streetname = streetname0+streetname1
    
    for direction in street_direction:
        if direction in currentaddress_lower_list:
            Current_Street_Direction = direction
            
            
        
            
    for streettype in street_type:        
        if streettype in currentaddress_lower_list:
            Current_Street_Type = streettype
            

    df2 = df2.append({'streetnumber': currentaddress.split(" ")[0], 'direction': Current_Street_Direction, 'streetname':streetname, 'streettype':Current_Street_Type}, ignore_index=True)
    streetname0=""
    streetname1=""
    Current_Street_Type=""
    Current_Street_Direction=""

i = 0

df2 = pd.concat([df2, df['State'],df['County']], axis=1)


    
def process_assessor(row, semaphore):
    # Process the address here

   
    Assessor_Info_dict = {
        'Tax_ID': [],
        'Owner_Name_1': [],
        'Owner_Name_2': []
    }
    Tax_ID = ""
    Owner_Name_1 = ""
    Owner_Name_2 = ""

    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_experimental_option(
        "prefs", {"download.default_directory": "‪F:\\Kartheek\\Download_Python"})

    # install webdriver
    driver = webdriver.Chrome(options=chromeOptions)

    driver.maximize_window()



    wait = WebDriverWait(driver, 60)

    parent_dir = "F:\\Kartheek\\Property-Info\\"
    searchAddress = f"{row.streetnumber} {row.streetname}"
    currentstreetnumber = row.streetnumber
    currentstreetname = row.streetname
    fullsearchaddress = f"{row.streetnumber} {row.direction} {row.streetname} {row.streettype} {row.State} {row.County}"
    
    os.makedirs(parent_dir + str(fullsearchaddress) + " - Automation Failed")
    directry = str(parent_dir + str(fullsearchaddress) + " - Automation Failed")
    
    #first value of state column
    current_state = row.State.lower()
    
    #first value of county column
    current_county = row.County.lower()
    
    state_county = f"{row.State}-{row.County}"
    # def handle_state_county_unavailable(fullsearchaddress):
    #     folder_path = "F:\Kartheek\Property-Info"
    #     target_folder_name = rf'{fullsearchaddress} - Automation Failed'
    #     new_folder_name = f'{fullsearchaddress}-Automation NA'
    #     search_and_rename_folder(folder_path, target_folder_name, new_folder_name)
    #     return

    # if not check_state_county_available(state_county):
    #     folder_path = "F:\Kartheek\Property-Info"
    #     target_folder_name = rf'{fullsearchaddress} - Automation Failed'
    #     new_folder_name = f'{fullsearchaddress}-Automation NA'
    #     search_and_rename_folder(folder_path, target_folder_name, new_folder_name)
    #     continue

    


    if not check_state_county_available(state_county):
        handle_state_county_unavailable(fullsearchaddress)
        semaphore.release()
        return False


    

    
    #open county information sheet
    with open("F:\\Kartheek\\Search-Data\\3 County information-FL.csv") as csv_file:
        county_info = csv.DictReader(csv_file) 
        
        #search for state county combination row - and save it in county_row_data
        for county_info_row in county_info:
            if county_info_row["State-County"].lower() == current_state+"-"+current_county:
               county_row_data = county_info_row
               enter_address = 0
               printing = 0
               results_ass = 0
               popupwindow = 0
               
           
    
    #get URL and open           
    driver.get(county_row_data["URL"])
    main_window = driver.current_window_handle
    time.sleep(3)
    
    if county_row_data["website_load_wait"] != "":
        time.sleep(int(county_row_data["website_load_wait"]))
        
    if county_row_data["Search_Records_Attribute_Value"] != "":
        driver.find_elements(By.LINK_TEXT, "Search Records").click()
    
        #wait.until(
            #EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Records_Attribute_Value"]))).click()
    
    if county_row_data["Disclaimer_Required"] == "Yes":
        time.sleep(2)
        wait.until(
            EC.visibility_of_element_located((By.XPATH,county_row_data["Disclaimer_Attribute_value"]))).click()
        
    if county_row_data["Pop-Up"] == "Yes":
        alert = driver.switch_to.alert
        time.sleep(1)
        alert.accept()
    
    if county_row_data["Terms_and_conditions_Attribute_Value"] != "":
        wait.until(
            EC.visibility_of_element_located((By.XPATH,county_row_data["Terms_and_conditions_Attribute_Value"]))).click()
    
    if county_row_data["Select_category"] != "":
        wait.until(
            EC.visibility_of_element_located((By.XPATH,county_row_data["Select_category"]))).click()        
        
    if county_row_data["Select_Address_Option_Attribute_Value"] != "":
        wait.until(
            EC.visibility_of_element_located((By.XPATH,county_row_data["Select_Address_Option_Attribute_Value"]))).click()
    
    #Search Address Field
    if county_row_data["Property_Address_Field_Attribute_Value"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Property_Address_Field_Attribute_Value"]))).click()
        #Enter Property Address to be searched
        
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Property_Address_Field_Attribute_Value"]))).send_keys(searchAddress)
        #Press Enter to search
        time.sleep(5)
        
    
    if county_row_data["Street_Number_Attribute_Value"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Street_Number_Attribute_Value"]))).click()
        
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Street_Number_Attribute_Value"]))).send_keys(currentstreetnumber)
    
    if county_row_data["Street_Name_Attribute_Value"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Street_Name_Attribute_Value"]))).click()
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Street_Name_Attribute_Value"]))).send_keys(currentstreetname)
        time.sleep(2)
    
    if county_row_data["Click_to_handle_element_not_clickable"] != "":
        driver.find_element(By.XPATH, county_row_data["Click_to_handle_element_not_clickable"]).click()
    
    #driver.find_element_by_xpath(county_row_data["Search_Button_Attribute_Value"]).click()
    if county_row_data["Search_Button_Attribute_Value"] != "":
        time.sleep(2)
        driver.find_element(By.XPATH, county_row_data["Search_Button_Attribute_Value"]).click()
    
    #results retrieved page
    
    
    if county_row_data["Results_Retrieved_Attribute_Value"] != "":
        time.sleep(3)
        
        try:
            driver.find_element(By.XPATH,county_row_data["Results_Retrieved_Attribute_Value"]).click()
        except:
            pass
    
    
    if county_row_data["Expand_Button_Attribute_value"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Expand_Button_Attribute_value"]))).click()
    
    time.sleep(5)
    if county_row_data["Copy_Tax_ID"] != "":
        
        Tax_ID = driver.find_element(By.XPATH,county_row_data["Copy_Tax_ID"]).text
        
        
    if county_row_data["Copy_Owner_Name_1"] != "":
        
        Owner_Name_1 = driver.find_element(By.XPATH,county_row_data["Copy_Owner_Name_1"]).text
        
        
    if county_row_data["Copy_Owner_Name_2"] != "":
        
        Owner_Name_2 = driver.find_element(By.XPATH,county_row_data["Copy_Owner_Name_2"]).text
        
        
    Assessor_Info_dict['Tax_ID'].append(Tax_ID)
    Assessor_Info_dict['Owner_Name_1'].append(Owner_Name_1)
    Assessor_Info_dict['Owner_Name_2'].append(Owner_Name_2)
    
    
    
    df_Assessor = pd.DataFrame(Assessor_Info_dict)
    
    var1 = searchAddress+"_Assessor_Data"
    excel_filename = var1 + ".xlsx"
    
    file_path = rf'F:\Kartheek\Property-Info\{fullsearchaddress} - Automation Failed\{excel_filename}'
    
    
    df_Assessor.to_excel(file_path, index=False)
    
    if county_row_data["Print_Button_Time_Delay"] != "":
        time.sleep(int(county_row_data["Print_Button_Time_Delay"]))
        
    #click on print button
    if county_row_data["Print_Button_Attribute_value"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Print_Button_Attribute_value"]))).click()
        
    window_handles = driver.window_handles
    
    if len(window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])
        
    if county_row_data["Print_Options_Attribute_Value"] != "":
        wait.until(
            EC.visibility_of_element_located((By.XPATH,county_row_data["Print_Options_Attribute_Value"]))).click()
        
    if county_row_data["Select_Print_Options_Attribute_Value"] != "":
        wait.until(
            EC.visibility_of_element_located((By.XPATH,county_row_data["Select_Print_Options_Attribute_Value"]))).click()
        

    #switch to new tab
    if len(window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])
        
    #Print the page
    time.sleep(5)
    
    # if county_row_data["Print_dialogue_opened"] == "No":
    #     if county_row_data["Final_Print_Time_Delay"] != "":
    #         time.sleep(int(county_row_data["Final_Print_Time_Delay"]))
        # Set the page size to 'A4'
        # result = driver.execute_cdp_cmd('Page.printToPDF', {'format': 'A4'})
        # # Save the PDF content
        # pdf_content_base64  = result['data']
        # pdf_content = base64.b64decode(pdf_content_base64)
        
        
    # if county_row_data["Print_dialogue_opened"] == "Yes":
    #     if county_row_data["Final_Print_Time_Delay"] != "":
    #         time.sleep(int(county_row_data["Final_Print_Time_Delay"]))
    if county_row_data["Final_Print_a_pdfviewer"] == "Yes":
        if county_row_data["Final_Print_Time_Delay"] != "":
            time.sleep(int(county_row_data["Final_Print_Time_Delay"]))
        current_url = driver.current_url
        var = searchAddress+"_Assessor"
        filename = var + ".pdf"
        
        path_and_filename = rf'F:\Kartheek\Property-Info\{fullsearchaddress} - Automation Failed\{filename}'
        pdf_viewer_print(path_and_filename, current_url)
        
    if county_row_data["Final_Print_a_pdfviewer"] == "No":
        if county_row_data["Final_Print_Time_Delay"] != "":
            time.sleep(int(county_row_data["Final_Print_Time_Delay"]))
        var = searchAddress+"_Assessor"
        filename = var + ".pdf"

        path_and_filename = rf'F:\Kartheek\Property-Info\{fullsearchaddress} - Automation Failed\{filename}'
        result = driver.execute_cdp_cmd('Page.printToPDF', {'format': 'A4'})
        # Save the PDF content
        non_pdf_viewer_print(result,path_and_filename)
    
    if len(window_handles) > 1:
        driver.close()
    
    
    
    # if current_county.lower() == 'miami-dade':
    #     driver.switch_to.window(driver.window_handles[-1])
    #     driver.close()
    
    #switch browsertab
    driver.switch_to.window(driver.window_handles[0])
    
    if len(os.listdir(rf"F:\Kartheek\Property-Info\{fullsearchaddress} - Automation Failed")) != 0:
        time.sleep(10)
    # Usage example
        folder_path = "F:\Kartheek\Property-Info"
        target_folder_name = rf'{fullsearchaddress} - Automation Failed'
        new_folder_name = f'{fullsearchaddress}'
        time.sleep(5)
        search_and_rename_folder(folder_path, target_folder_name, new_folder_name)
    
    
    
    driver.quit()
    Process_Search(Owner_Name_1, Owner_Name_2, county_row_data, fullsearchaddress)
    semaphore.release()
    
    
    
#pool = multiprocessing.Pool()
#addresses = [(row.streetnumber, row.streetname) for row in df2.itertuples(index=False)]
#pool.map(process_address, addresses)

def Process_Search(Owner_Name_1, Owner_Name_2, county_row_data, fullsearchaddress):
    #to make the name searchable
    x = re.search("EST",Owner_Name_1)
    if x == None:
        Owner_Name_1 = Owner_Name_1[0:-1].strip()
    else:
        Owner_Name_1 = Owner_Name_1.split(" ")[0] + " "+ Owner_Name_1.split(" ")[1]
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_experimental_option(
        "prefs", {"download.default_directory": "‪F:\\Kartheek\\Download_Python"})

    # install webdriver
    driver = webdriver.Chrome(options=chromeOptions)

    driver.maximize_window()
    wait = WebDriverWait(driver, 60)
    
    if county_row_data["Search_URL"] != "":
        driver.get(county_row_data["Search_URL"])
    
    if county_row_data["Search_Click_On_Name_Search_Option"] != "":
        wait.until(
            EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Click_On_Name_Search_Option"]))).click()

    if county_row_data["Search_Click_On_Disclaimer"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Click_On_Disclaimer"]))).click()

    if county_row_data["Search_Enter_Owner_Name_LastName_Comma_FirstName"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Enter_Owner_Name_LastName_Comma_FirstName"]))).click()    
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Enter_Owner_Name_LastName_Comma_FirstName"]))).send_keys(Owner_Name_1)
        time.sleep(5)
    if county_row_data["Search_Enter_Last_Name"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Enter_Last_Name"]))).click()    
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Enter_Last_Name"]))).send_keys(Owner_Name_1[0:-1].strip())
    
    if county_row_data["Search_Enter_First_Name"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Enter_First_Name"]))).click()    
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Enter_First_Name"]))).send_keys(Owner_Name_1[0:-1].strip())
        
    if county_row_data["Search_Record_From_Date"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Record_From_Date"]))).click()
        
    if county_row_data["Search_Record_To_Date"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Record_To_Date"]))).click()
    
    if county_row_data["Search_Searchbutton"] != "":
        time.sleep(2)
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Searchbutton"]))).click()
        time.sleep(5)
        
    if county_row_data["Search_Select_Names_from_popup"] != "":
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Select_Names_from_popup"]))).click()
    
    if county_row_data["Search_Pushpin_to_hide_searchoptions"] != "":
        time.sleep(5)
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Pushpin_to_hide_searchoptions"]))).click()        
    
    if county_row_data["Search_select_click_Items_per_page_option"] != "":
        time.sleep(5)
        wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_select_click_Items_per_page_option"]))).click()
    
    if county_row_data["Search_select_max_Items_per_page_option"] != "":
        time.sleep(5)
        driver.find_element(By.XPATH,county_row_data["Search_select_max_Items_per_page_option"]).click()
        
    if county_row_data["Search_Effective_Date"] != "":
        driver.find_element(By.XPATH,county_row_data["Search_Effective_Date"]).click()
        Eff_Date = driver.find_element(By.XPATH,county_row_data["Search_Effective_Date"]).text
        # Define a regular expression pattern to capture the date after "Released through date:"
        pattern = r"Released through date: (\d{2}/\d{2}/\d{4})"
        # Use the search() function to find the first occurrence of the pattern in the text
        match = re.search(pattern, Eff_Date)
        if match:
            Eff_Date = match.group(1) # here group(1) will give only the first date found from start
            print(Eff_Date)
        else:
            print("No date found.")
    
    if county_row_data["Search_No_of_records"] != "":
        No_of_Records = wait.until(
        EC.visibility_of_element_located((By.XPATH,county_row_data["Search_No_of_records"]))).text
        # Define a regular expression pattern to capture the last two or three digits
        res = [int(i) for i in No_of_Records.split() if i.isdigit()]
        No_of_Records = int(res[2])
        
        
    # if county_row_data["Search_Sort_by_recorded_date"] != "":
    #     time.sleep(10)
    #     wait.until(
    #     EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Sort_by_recorded_date"]))).click()        
        



    Party_Type_list = []
    Full_Name_list = []
    CrossPartyName_list = []
    RecordedDate_list = []
    DocumentType_list = []
    BookType_list = []
    BookPage_list = []
    Book_list = []
    Page_list = []
    ClerkFileNumber_list = []
    Consideration_list = []
    FirstLegalDescription_list = []
    Description2_list = []
    CaseNumber_list = []


    
    Document_Notes_Dict = {}

    Document_Notes_Dict = {'Part_Type':Party_Type_list, 'Full_Name':Full_Name_list, 'CrossPartyName':CrossPartyName_list, 'RecordedDate':RecordedDate_list, 'DocumentType':DocumentType_list, 'BookType':BookType_list, 'BookPage':BookPage_list, 'Book':Book_list, 'Page':Page_list, 'ClerkFileNumber':ClerkFileNumber_list, 'Consideration':Consideration_list, 'FirstLegalDescription':FirstLegalDescription_list, 'Description2':Description2_list, 'CaseNumber':CaseNumber_list}    
        
        
    for i in range(No_of_Records):
        Party_Type = " 0"
        Full_Name = " 0"
        CrossPartyName = " 0"
        RecordedDate = " 0"
        DocumentType = " 0"
        BookType = " 0"
        BookPage = " 0"
        Book = " 0"
        Page = " 0"
        ClerkFileNumber = " 0"
        Consideration = " 0"
        FirstLegalDescription = " 0"
        Description2 = " 0"
        CaseNumber = " 0"

        time.sleep(5)
    #Search_Results_Row_Party_Type1 = driver.find_element(By.XPATH,xpath).text        
        #print(Search_Results_Row_Party_Type1)
        
        
        if county_row_data["Search_Results_Row_Party_Type"] != "":
            
            x0 = county_row_data["Search_Results_Row_Party_Type"].format(i+1)
            try:
                Party_Type = driver.find_element(By.XPATH,x0).text
                
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_FullName"] != "":
            x1 = county_row_data["Search_Results_Row_FullName"].format(i+1)
            try:        
                Full_Name = driver.find_element(By.XPATH,x1).text
                
            except:
                pass
        
        if county_row_data["Search_Results_Row_CrossPartyName"] != "":
            x2 = county_row_data["Search_Results_Row_CrossPartyName"].format(i+1)
            try:
                CrossPartyName = driver.find_element(By.XPATH,x2).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_RecordedDate"] != "":
            x3 = county_row_data["Search_Results_Row_RecordedDate"].format(i+1)
            try:
                RecordedDate = driver.find_element(By.XPATH,x3).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_DocumentType"] != "":
            x4 = county_row_data["Search_Results_Row_DocumentType"].format(i+1)
            try:
                DocumentType = driver.find_element(By.XPATH,x4).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_BookType"] != "":
            x5 = county_row_data["Search_Results_Row_BookType"].format(i+1)
            try:
                BookType =  driver.find_element(By.XPATH,x5).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_BookPage"] != "":
            x6 = county_row_data["Search_Results_Row_BookPage"].format(i+1)
            try:
                BookPage = driver.find_element(By.XPATH,x6).text
                
            except:
                pass    
        
        if county_row_data["Search_Results_Row_Book"] != "":
            x7 = county_row_data["Search_Results_Row_Book"].format(i+1)
            try:
                Book = driver.find_element(By.XPATH,x7).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_Page"] != "":
            x8 = county_row_data["Search_Results_Row_Page"].format(i+1)
            try:
                Page = driver.find_element(By.XPATH,x8).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_ClerkFileNumber"] != "":
            x9 = county_row_data["Search_Results_Row_ClerkFileNumber"].format(i+1)
            try:
                ClerkFileNumber = driver.find_element(By.XPATH,x9).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_Consideration"] != "":
            x10 = county_row_data["Search_Results_Row_Consideration"].format(i+1)
            try:
                Consideration = driver.find_element(By.XPATH,x10).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_FirstLegalDescription"] != "":
            x11 = county_row_data["Search_Results_Row_FirstLegalDescription"].format(i+1)
            try:
                FirstLegalDescription = driver.find_element(By.XPATH,x11).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_Description2"] != "":
            x12 = county_row_data["Search_Results_Row_Description2"].format(i+1)
            try:
                Description2 = driver.find_element(By.XPATH,x12).text
                
            except:
                pass
            
        if county_row_data["Search_Results_Row_CaseNumber"] != "":
            x13 = county_row_data["Search_Results_Row_CaseNumber"].format(i+1)
            try:
                CaseNumber = driver.find_element(By.XPATH,x13).text
                
            except:
                pass
        
        
        
        
        if county_row_data["Search_Record_Row_to_view_image"] != "":
            x14 = county_row_data["Search_Record_Row_to_view_image"].format(i+1)
            try:
                driver.find_element(By.XPATH,x14).click()
                
            except:
                pass
        Party_Type_list.append(Party_Type)
        Full_Name_list.append(Full_Name)
        CrossPartyName_list.append(CrossPartyName)
        RecordedDate_list.append(RecordedDate)
        DocumentType_list.append(DocumentType)
        BookType_list.append(BookType)
        BookPage_list.append(BookPage)
        Book_list.append(Book)
        Page_list.append(Page)
        ClerkFileNumber_list.append(ClerkFileNumber)
        Consideration_list.append(Consideration)
        FirstLegalDescription_list.append(FirstLegalDescription)
        Description2_list.append(Description2)
        CaseNumber_list.append(CaseNumber)
        
        #wait.until(EC.visibility_of_element_located((By.XPATH,))).click()
        time.sleep(3)
        # if len(window_handles) > 1:
        #     driver.switch_to.window(driver.window_handles[-1])
        # time.sleep(10)
        if county_row_data["Search_Click_On_Image_Viewer"] != 0:
            wait.until(EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Click_On_Image_Viewer"]))).click()
        time.sleep(10)
        window_handles = driver.window_handles
        if len(window_handles) > 1:
            time.sleep(2)
            driver.switch_to.window(driver.window_handles[-1])
            
        current_url = driver.current_url
        var = 'doc'+rf"{i+1}"+"-"+DocumentType+"-"+ClerkFileNumber
        filename = var+".pdf"
        path_and_filename = rf'F:\Kartheek\Property-Info\{fullsearchaddress}\{filename}'
        pdf_viewer_print(path_and_filename, current_url)
        
        
        # var = 'doc{i+1}'
        # filename = var+".pdf"
        
        # path_and_filename = rf'F:\Kartheek\Property-Info\{fullsearchaddress}\{filename}'
        # result = driver.execute_cdp_cmd('Page.printToPDF', {'format': 'A4'})
        # # Save the PDF content
        # non_pdf_viewer_print(result,path_and_filename)
        
        if len(window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        #wait.until(EC.visibility_of_element_located((By.XPATH,county_row_data["Search_Click_On_Image_Viewer"]))).click()
        #current_url = driver.current_url
        #print("before path name")
        #path_and_filename = rf'F:\Kartheek\Property-Info\{fullsearchaddress}\\{Owner_Name_1}{i}-{ClerkFileNumber}.pdf'
        #pdf_viewer_print(path_and_filename,current_url)
       
    Document_Notes_Dict = {'Part_Type':Party_Type_list, 'Full_Name':Full_Name_list, 'CrossPartyName':CrossPartyName_list, 'RecordedDate':RecordedDate_list, 'DocumentType':DocumentType_list, 'BookType':BookType_list, 'BookPage':BookPage_list, 'Book':Book_list, 'Page':Page_list, 'ClerkFileNumber':ClerkFileNumber_list, 'Consideration':Consideration_list, 'FirstLegalDescription':FirstLegalDescription_list, 'Description2':Description2_list, 'CaseNumber':CaseNumber_list}    
    
    Index_Data_Frame = pd.DataFrame.from_dict(Document_Notes_Dict)
    
    
    Index_Data_Frame.to_csv(rf'F:\\Kartheek\\Property-Info\\{fullsearchaddress}\\{Owner_Name_1}.csv')
    time.sleep(5)
    
    
    
    #Owner_Name_2[0:-1].strip()
    driver.close()

if __name__ == '__main__':
    addresses = list(df2.itertuples(index=False))
    semaphore = threading.BoundedSemaphore(3)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        counter = 0

        while counter < len(addresses):
            for _ in range(3):
                if counter >= len(addresses):
                    break
                address = addresses[counter]
                semaphore.acquire()
                future = executor.submit(process_assessor, address, semaphore)
                futures.append(future)
                counter += 1
                
            completed_futures = []
            for future in futures:
                if future.done():
                    future.result()
                    completed_futures.append(future)

            for completed_future in completed_futures:
                futures.remove(completed_future)
    

    
    