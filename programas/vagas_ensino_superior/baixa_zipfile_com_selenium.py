from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time

# Path to your ChromeDriver
chrome_driver_path = '/path/to/chromedriver'

# Set up the Chrome driver
driver = webdriver.Chrome()

# URL to download
url = "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2022.zip"

# Open the browser and navigate to the URL
driver.get(url)

# Wait for a while to ensure the download starts
time.sleep(10)  # Adjust the sleep time as necessary

# Optionally, check for any popups, redirects, etc.
# If the download doesn't start automatically, you might need to trigger it manually.

# Close the browser after download
driver.quit()
