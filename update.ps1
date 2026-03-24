.\venv\Scripts\Activate.ps1
git stash save
git pull
pip install -r requirements.txt
python scripts/build_owned_listings_dump.py 
echo "update complete"