from PIL import Image, ImageEnhance
import os

def enhance_image(input_path, output_path, size=(1200, 1200)):
    img = Image.open(input_path)
    
    # Resize
    img = img.resize(size, Image.LANCZOS)
    
    # Enhance brightness and sharpness
    enhancer = ImageEnhance.Brightness(img)
    img = enhancer.enhance(1.2)  # 1.0 = original, >1 = brighter

    enhancer = ImageEnhance.Sharpness(img)
    img = enhancer.enhance(2.0)  # 1.0 = original, >1 = sharper

    img.save(output_path)
    print(f"Saved enhanced image to {output_path}")
