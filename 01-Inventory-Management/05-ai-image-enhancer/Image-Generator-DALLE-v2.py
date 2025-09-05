import os
import openai
import requests
from PIL import Image

# Set your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY") or "your-api-key-here"

# Input/output folders
input_dir = "unprocessed Images"
output_dir = "Improved Images"
os.makedirs(output_dir, exist_ok=True)

# Resize to 1024x1024 for DALL·E variation API
def prepare_image_for_dalle_variation(path):
    img = Image.open(path).convert("RGB")
    img = img.resize((1024, 1024), Image.LANCZOS)
    temp_path = path.replace(".jpg", "_resized.png").replace(".jpeg", "_resized.png").replace(".png", "_resized.png")
    img.save(temp_path, format="PNG")
    return temp_path

# Process each image in the folder
for filename in os.listdir(input_dir):
    if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
        original_path = os.path.join(input_dir, filename)
        print(f"Enhancing with DALL·E: {filename}")

        # Resize and convert to PNG
        resized_path = prepare_image_for_dalle_variation(original_path)

        # Open and send to DALL·E variation API
        with open(resized_path, "rb") as image_file:
            response = openai.Image.create_variation(
                image=image_file,
                n=1,
                size="1024x1024"
            )

        # Download and save enhanced image
        image_url = response['data'][0]['url']
        img_data = requests.get(image_url).content
        name, ext = os.path.splitext(filename)
        output_filename = f"{name}-Improved.png"
        output_path = os.path.join(output_dir, output_filename)

        with open(output_path, "wb") as handler:
            handler.write(img_data)

        print(f"✅ Saved: {output_filename}")
        os.remove(resized_path)
