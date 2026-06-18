import os
import json
import re
import requests
from io import BytesIO
from PIL import Image

def sanitize_name(name):
    name = name.lower()
    name = name.replace(" • ", "_").replace(" & ", "_").replace("&", "_").replace(" •", "_").replace("• ", "_").replace("•", "_")
    name = re.sub(r'[^a-z0-9_ -]', '', name)
    name = name.replace(" ", "_").replace("-", "_")
    name = re.sub(r'_+', '_', name)
    return name.strip('_')

def download_and_convert():
    icons_path = "character_icons.json"
    if not os.path.exists(icons_path):
        print(f"[ERROR] {icons_path} not found.")
        return

    with open(icons_path, "r", encoding="utf-8") as f:
        icons = json.load(f)
    
    output_dir = os.path.join("docs", "assets", "icons")
    os.makedirs(output_dir, exist_ok=True)
    new_icons = {}

    print(f"[INFO] Found {len(icons)} character icons to process...")

    for name, url in icons.items():
        if url.startswith("assets/icons/") or url.startswith("docs/assets/icons/"):
            print(f"Skipping already localized: {name}")
            new_icons[name] = url
            continue

        safe_name = sanitize_name(name)
        dest_filename = f"{safe_name}.webp"
        dest_path = os.path.join(output_dir, dest_filename)

        if not os.path.exists(dest_path):
            print(f"Downloading {name} from {url}...")
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                img = Image.open(BytesIO(response.content))
                
                if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                    img.save(dest_path, "WEBP", quality=90)
                else:
                    img.convert("RGB").save(dest_path, "WEBP", quality=90)
                print(f"  -> Saved as {dest_path}")
            except Exception as e:
                print(f"  -> [ERROR] Failed to download/convert {name}: {e}")
                new_icons[name] = url
                continue
        else:
            print(f"Icon already exists locally: {name}")
        
        new_icons[name] = f"assets/icons/{dest_filename}"

    with open(icons_path, "w", encoding="utf-8") as f:
        json.dump(new_icons, f, indent=2, ensure_ascii=False)
    print(f"[SUCCESS] Updated {icons_path} mappings.")

if __name__ == "__main__":
    download_and_convert()
