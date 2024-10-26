import os
import base64


def images_to_base64(directory):
    base64_images = {}

    for filename in os.listdir(directory):
        # Check if the file is an image (you can add more extensions if needed)
        if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
            filepath = os.path.join(directory, filename)
            with open(filepath, "rb") as image_file:
                # Read and encode the image to Base64
                encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
                # Store in dictionary with filename as key
                base64_images[filename] = encoded_string

    return base64_images


# Usage example:
directory_path = "./data_dirs/ready_screenshots"
images_base64_dict = images_to_base64(directory_path)

# To print or check the result for a specific file:
for filename, base64_string in images_base64_dict.items():
    print(f"{filename}: {base64_string[:30]}...")  # Printing only the first 30 characters for brevity
