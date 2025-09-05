# main.py
import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# --- ⚠️ CONFIGURATION - EDIT THESE VALUES ---
CANVA_EMAIL = "samar.khader@gmail.com"
CANVA_PASSWORD = "Sk6478309277" # WARNING: Storing passwords in scripts is insecure.
IMAGE_FOLDER = "Unprocessed image-2025-07-17_12-12-20"
DESIGN_WIDTH = 1200
DESIGN_HEIGHT = 1200
# ---------------------------------------------

# --- Initialize WebDriver ---
driver = webdriver.Chrome()
wait = WebDriverWait(driver, 30) # Wait up to 30 seconds for elements

try:
    # 1. LOG IN TO CANVA
    # =================================================================
    print("🚀 Starting Canva automation...")
    driver.get("https://www.canva.com/login")
    driver.maximize_window()

    # --- NEW: Handle Cookie Consent Banner ---
    try:
        # Wait a few seconds to see if a cookie banner appears
        print("→ Checking for cookie banner...")
        # Note: The text on the button might be different (e.g., "Allow all", "Agree").
        # If this fails, inspect the button on the website and update the text here.
        cookie_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept all cookies')]"))
        )
        cookie_button.click()
        print("✅ Accepted cookies.")
        time.sleep(1) # Give a moment for the banner to disappear
    except TimeoutException:
        # If no cookie banner is found after 5 seconds, that's fine. Just continue.
        print("→ No cookie banner found, proceeding.")
    # ---------------------------------------------
    
    print("→ Clicking 'Continue with email'...")
    continue_with_email_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Continue with email')]"))
    )
    continue_with_email_button.click()
    
    print("→ Entering login credentials...")
    email_input = wait.until(EC.element_to_be_clickable((By.ID, "email")))
    email_input.send_keys(CANVA_EMAIL)
    
    driver.find_element(By.XPATH, "//button[@type='submit']").click()
    
    password_input = wait.until(EC.element_to_be_clickable((By.ID, "password")))
    password_input.send_keys(CANVA_PASSWORD)

    driver.find_element(By.XPATH, "//button[@type='submit']").click()
    print("✅ Login successful!")

    # 2. CREATE A NEW DESIGN
    # =================================================================
    print("\n→ Creating a new design...")
    create_design_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Create a design')]"))
    )
    create_design_button.click()

    custom_size_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Custom size')]"))
    )
    custom_size_button.click()

    width_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@name='width']")))
    width_input.send_keys(str(DESIGN_WIDTH))

    height_input = driver.find_element(By.XPATH, "//input[@name='height']")
    height_input.send_keys(str(DESIGN_HEIGHT))
    
    driver.find_element(By.XPATH, "//div[contains(@class, 'Dialog')]//button[contains(., 'Create new design')]").click()
    print(f"✅ Design created with size {DESIGN_WIDTH}x{DESIGN_HEIGHT}.")

    # 3. UPLOAD IMAGES AND ADD TO PAGES
    # =================================================================
    print("\n→ Switching to the editor tab...")
    time.sleep(5) 
    driver.switch_to.window(driver.window_handles[-1])

    image_files = [f for f in os.listdir(IMAGE_FOLDER) if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))]
    if not image_files:
        print("❌ No images found in the specified folder. Exiting.")
        exit()

    print(f"Found {len(image_files)} images to upload.")

    for i, image_name in enumerate(image_files):
        print(f"\nProcessing image {i+1}/{len(image_files)}: {image_name}")
        image_path = os.path.join(IMAGE_FOLDER, image_name)
        
        upload_input = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='file' and contains(@id, 'upload')]"))
        )
        upload_input.send_keys(image_path)
        print("→ Uploading...")
        
        try:
            image_thumbnail = wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//img[contains(@alt, '{os.path.splitext(image_name)[0]}')]"))
            )
            print("→ Adding image to page...")
            image_thumbnail.click()
            print(f"✅ Added {image_name} to page {i + 1}.")
        except TimeoutException:
            print(f"⚠️ Timed out waiting for '{image_name}' to upload. Skipping.")
            continue

        if i < len(image_files) - 1:
            try:
                time.sleep(2)
                add_page_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Add page')]"))
                )
                add_page_button.click()
                print("→ Added a new page.")
                time.sleep(1)
            except TimeoutException:
                print("⚠️ Could not find the 'Add page' button. Stopping.")
                break

except TimeoutException as e:
    print(f"\n❌ A timeout error occurred: The script could not find an element in time.")
    print("This usually means the website layout has changed or the connection is slow.")
    print(f"Error details: {e}")
except Exception as e:
    print(f"\n❌ An unexpected error occurred: {e}")

finally:
    print("\n🎉 Script finished.")
    time.sleep(15) 
    driver.quit()