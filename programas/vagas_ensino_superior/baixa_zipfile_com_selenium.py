from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time,os

downloaded_files_dir: str = os.path.join(os.getcwd(),"tempfiles")
print(downloaded_files_dir)

chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": downloaded_files_dir,  # Set the download directory
    "download.prompt_for_download": False,  # Disable the prompt for download
    "download.directory_upgrade": True,  # Ensure directory upgrade
    "safebrowsing.enabled": True  # Enable safe browsing
})

# Set up the Chrome driver
driver = webdriver.Chrome(options=chrome_options)


# URL to download
url = "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2022.zip"

# Open the browser and navigate to the URL
driver.get(url)
time.sleep(5)

if not os.path.isdir(downloaded_files_dir):
   os.mkdir(downloaded_files_dir)

files_in_dir:list[str] =  os.listdir(downloaded_files_dir)
print(files_in_dir)

while not any(map(lambda x: ".zip" in x , files_in_dir)):
   print("checando zipdnv")
   time.sleep(5)
   files_in_dir = os.listdir(downloaded_files_dir)
   print(files_in_dir)


# Wait for a while to ensure the download starts
time.sleep(10)  # Adjust the sleep time as necessary

# Optionally, check for any popups, redirects, etc.
# If the download doesn't start automatically, you might need to trigger it manually.

# Close the browser after download
driver.quit()
