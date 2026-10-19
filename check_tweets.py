#!/usr/bin/env python3
"""Quick check of bankrbot tweets"""
import sys
from pathlib import Path
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.database.models import Database

db = Database()
db.connect()
cursor = db.conn.cursor()

# Get sample tweets from bankrbot
cursor.execute("""
    SELECT id, text, created_at 
    FROM tweets 
    WHERE LOWER(author_username) = 'bankrbot' 
    ORDER BY created_at DESC
    LIMIT 20
""")

print("\n=== RECENT BANKRBOT TWEETS ===\n")
for row in cursor.fetchall():
    tweet_id, text, created_at = row
    print(f"ID: {tweet_id}")
    print(f"Time: {created_at}")
    print(f"Text: {text[:300]}")
    print("-" * 60)

# Check for any tweet containing "deploy" case insensitive
cursor.execute("""
    SELECT COUNT(*) 
    FROM tweets 
    WHERE LOWER(author_username) = 'bankrbot' 
    AND LOWER(text) LIKE '%deploy%'
""")
deploy_count = cursor.fetchone()[0]
print(f"\n\nTweets with 'deploy': {deploy_count}")

# Check what patterns exist
cursor.execute("""
    SELECT text 
    FROM tweets 
    WHERE LOWER(author_username) = 'bankrbot' 
    AND LOWER(text) LIKE '%deploy%'
    LIMIT 5
""")
print("\n=== SAMPLE DEPLOY TWEETS ===")
for row in cursor.fetchall():
    print(f"\n{row[0][:400]}")
    print("-" * 60)

db.close()
