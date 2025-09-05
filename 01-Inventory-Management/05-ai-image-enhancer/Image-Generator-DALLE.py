import os
import openai
import requests
from PIL import Image
from io import BytesIO

# Set your OpenAI API Key
openai.api_key = os.getenv("OPENAI_API_KEY") or "your-api-key-here"

# Directories
input_dir = "unprocessed Images"
output_dir = "Improved Images"
os.makedirs(output_dir, exist_ok=True)

# Resize utility to ensure 1024x1024 for DALL·E inpainting
def prepare_image_for_dalle(path):
    img = Image.open(path).convert("RGB")
    img = img.resize((1024, 1024), Image.LANCZOS)
    temp_path = path.replace(".png", "_resized.png")
    img.save(temp_path)
    return temp_path

# Loop through images
for filename in os.listdir(input_dir):
    if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
        original_path = os.path.join(input_dir, filename)
        print(f"Enhancing with DALL·E: {filename}")

        # Resize to 1024x1024 for DALL·E
        resized_path = prepare_image_for_dalle(original_path)

        # Upload the image to OpenAI
        with open(resized_path, "rb") as image_file:
            response = openai.images.edit(
                image=image_file,
                mask=None,  # No mask: enhance the whole image
                prompt="Enhance this product image for e-commerce, 1200x1200, professional studio lighting, clear sharp detail",
                n=1,
                size="1024x1024"  # DALL·E supports only 1024x1024 for editing
            )

        image_url = response['data'][0]['url']

        # Download the image
        img_data = requests.get(image_url).content
        name, ext = os.path.splitext(filename)
        output_filename = f"{name}-Improved.png"
        output_path = os.path.join(output_dir, output_filename)

        with open(output_path, "wb") as handler:
            handler.write(img_data)

        print(f"Saved enhanced image to: {output_filename}")
        os.remove(resized_path)  # Clean up temp file

