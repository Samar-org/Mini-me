
import os
from PIL import Image, ImageEnhance
from pathlib import Path

input_dir = Path(r"C:\0001-Samar-MiniMe\01-Inventory-Management\04-product-images-production\unprocessed images")
output_dir = Path(r"C:\0001-Samar-MiniMe\01-Inventory-Management\04-product-images-production\processed images")
output_dir.mkdir(parents=True, exist_ok=True)

def enhance_and_resize_image(input_path, output_path, size=(1200, 1200)):
    with Image.open(input_path) as img:
        img = img.convert("RGB")
        img.thumbnail(size, Image.LANCZOS)
        new_img = Image.new("RGB", size, (255, 255, 255))
        paste_position = (
            (size[0] - img.width) // 2,
            (size[1] - img.height) // 2
        )
        new_img.paste(img, paste_position)
        enhancer = ImageEnhance.Brightness(new_img)
        new_img = enhancer.enhance(1.1)
        enhancer = ImageEnhance.Sharpness(new_img)
        new_img = enhancer.enhance(2.0)
        new_img.save(output_path, format="JPEG", quality=90)

for file in os.listdir(input_dir):
    if file.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
        input_path = input_dir / file
        output_path = output_dir / f"{Path(file).stem}.jpg"
        enhance_and_resize_image(input_path, output_path)
        print(f"Processed: {file}")
