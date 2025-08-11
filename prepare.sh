# Project structure setup script
# Run this on your Ubuntu server

# Create project directories
mkdir -p /bodega/procesador
cd /bodega/procesador

# Create directory structure
mkdir -p {uploads,safe_storage,exports,logs}
mkdir -p {scripts,web_app,templates,static}

# Directory structure:
# /mnt/hdd/data_processor/
# ├── uploads/           # Temporary upload location
# ├── safe_storage/      # Permanent file storage
# │   ├── txt_files/     # One-time TXT files
# │   └── xlsx_files/    # Daily XLSX files
# ├── exports/           # Generated CSV/XLSX exports
# ├── logs/              # Processing logs
# ├── scripts/           # Python ETL scripts
# ├── web_app/           # Flask application
# ├── templates/         # HTML templates
# └── static/            # CSS/JS files

# Set permissions
chmod 755 uploads safe_storage exports logs scripts web_app
chmod 777 uploads  # Upload directory needs write access

echo "Directory structure created!"
echo "Next: Install dependencies"
