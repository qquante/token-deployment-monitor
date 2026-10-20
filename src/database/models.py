"""
Database models for bankr-monitor.

SQLite database schema for storing tweets, deployments, and related data.
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any


# Default database path
DEFAULT_DB_PATH = Path(__file__).parent.parent.parent / "data" / "bankr_monitor.db"


class Database:
    """SQLite database manager for bankr-monitor."""
    
    def __init__(self, db_path: Optional[Path] = None):
        """Initialize database connection."""
        self.db_path = db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None
        
    def connect(self) -> sqlite3.Connection:
        """Connect to database."""
        if self.conn is None:
            self.conn = sqlite3.connect(str(self.db_path))
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def init_schema(self):
        """Initialize database schema."""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Tweets table - stores ALL raw tweets
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tweets (
                id TEXT PRIMARY KEY,
                url TEXT,
                twitter_url TEXT,
                text TEXT,
                source TEXT,
                retweet_count INTEGER,
                reply_count INTEGER,
                like_count INTEGER,
                quote_count INTEGER,
                view_count INTEGER,
                created_at TEXT,
                lang TEXT,
                bookmark_count INTEGER,
                is_reply INTEGER,
                in_reply_to_id TEXT,
                conversation_id TEXT,
                in_reply_to_user_id TEXT,
                in_reply_to_username TEXT,
                
                -- Author info
                author_id TEXT,
                author_username TEXT,
                author_name TEXT,
                author_followers INTEGER,
                author_is_blue_verified INTEGER,
                author_verified_type TEXT,
                author_is_automated INTEGER,
                author_automated_by TEXT,
                
                -- Raw JSON for complete data
                raw_json TEXT,
                
                -- Metadata
                rule_id TEXT,
                rule_tag TEXT,
                rule_value TEXT,
                received_at TEXT DEFAULT CURRENT_TIMESTAMP,
                
                -- Indexes for common queries
                UNIQUE(id)
            )
        """)
        
        # Deployments table - parsed deployment confirmations
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deployments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT UNIQUE,
                
                -- Deployment info
                token_name TEXT,
                token_symbol TEXT,
                contract_address TEXT,
                chain TEXT DEFAULT 'base',
                clanker_url TEXT,
                
                -- Requester info
                requester_username TEXT,
                requester_user_id TEXT,
                requester_name TEXT,
                
                -- Fee routing
                fee_recipient_username TEXT,
                fee_recipient_address TEXT,
                
                -- Timestamps
                deployed_at TEXT,
                received_at TEXT DEFAULT CURRENT_TIMESTAMP,
                
                -- Raw data
                raw_tweet_text TEXT,
                
                FOREIGN KEY (tweet_id) REFERENCES tweets(id)
            )
        """)
        
        # Deploy requests table - user requests before deployment
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deploy_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT UNIQUE,
                
                -- Request info
                requested_token_name TEXT,
                requested_ticker TEXT,
                requested_image_url TEXT,
                
                -- Requester
                requester_username TEXT,
                requester_user_id TEXT,
                
                -- Status tracking
                status TEXT DEFAULT 'pending',  -- pending, deployed, failed, rejected
                deployment_id INTEGER,
                
                -- Timestamps
                requested_at TEXT,
                received_at TEXT DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (tweet_id) REFERENCES tweets(id),
                FOREIGN KEY (deployment_id) REFERENCES deployments(id)
            )
        """)
        
        # Likes table - tracks user likes on PRE-BANKR tweets
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS likes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT NOT NULL,
                telegram_user_id TEXT NOT NULL,
                telegram_username TEXT,
                telegram_message_id INTEGER,
                liked_at TEXT DEFAULT CURRENT_TIMESTAMP,
                
                -- Prevent duplicate likes
                UNIQUE(tweet_id, telegram_user_id)
            )
        """)
        
        # User stats table - tracks deploys made and received per user
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_stats (
                username TEXT PRIMARY KEY,
                deploys_made INTEGER DEFAULT 0,
                deploys_received INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Add deploys_made column if it doesn't exist (for existing DBs)
        try:
            cursor.execute("ALTER TABLE user_stats ADD COLUMN deploys_made INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists
        
        # Bankr tweets table - stores raw tweets from @bankrbot for backfill
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bankr_tweets (
                id TEXT PRIMARY KEY,
                text TEXT,
                created_at TEXT,
                in_reply_to_id TEXT,
                in_reply_to_username TEXT,
                raw_json TEXT,
                processed INTEGER DEFAULT 0,
                agent_result TEXT,
                fee_recipients TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_author ON tweets(author_username)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_created ON tweets(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_rule ON tweets(rule_tag)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deployments_contract ON deployments(contract_address)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deployments_requester ON deployments(requester_username)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deployments_token ON deployments(token_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_likes_tweet ON likes(tweet_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_likes_user ON likes(telegram_user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_stats_received ON user_stats(deploys_received)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bankr_tweets_processed ON bankr_tweets(processed)")
        
        conn.commit()
        print(f"[DB] Schema initialized at {self.db_path}")
        
    def insert_tweet(self, tweet: Dict[str, Any], rule_id: str = "", 
                     rule_tag: str = "", rule_value: str = "") -> bool:
        """Insert a tweet into the database."""
        conn = self.connect()
        cursor = conn.cursor()
        
        author = tweet.get("author", {})
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO tweets (
                    id, url, twitter_url, text, source,
                    retweet_count, reply_count, like_count, quote_count, view_count,
                    created_at, lang, bookmark_count, is_reply,
                    in_reply_to_id, conversation_id, in_reply_to_user_id, in_reply_to_username,
                    author_id, author_username, author_name, author_followers,
                    author_is_blue_verified, author_verified_type,
                    author_is_automated, author_automated_by,
                    raw_json, rule_id, rule_tag, rule_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tweet.get("id"),
                tweet.get("url"),
                tweet.get("twitterUrl"),
                tweet.get("text"),
                tweet.get("source"),
                tweet.get("retweetCount", 0),
                tweet.get("replyCount", 0),
                tweet.get("likeCount", 0),
                tweet.get("quoteCount", 0),
                tweet.get("viewCount", 0),
                tweet.get("createdAt"),
                tweet.get("lang"),
                tweet.get("bookmarkCount", 0),
                1 if tweet.get("isReply") else 0,
                tweet.get("inReplyToId"),
                tweet.get("conversationId"),
                tweet.get("inReplyToUserId"),
                tweet.get("inReplyToUsername"),
                author.get("id"),
                author.get("userName"),
                author.get("name"),
                author.get("followers", 0),
                1 if author.get("isBlueVerified") else 0,
                author.get("verifiedType"),
                1 if author.get("isAutomated") else 0,
                author.get("automatedBy"),
                json.dumps(tweet, ensure_ascii=False),
                rule_id,
                rule_tag,
                rule_value
            ))
            conn.commit()
            return True
        except Exception as e:
            print(f"[DB ERROR] Insert tweet failed: {e}")
            return False
    
    def insert_deployment(self, tweet_id: str, token_name: str, 
                          contract_address: str, requester_username: str,
                          **kwargs) -> Optional[int]:
        """Insert a deployment record."""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO deployments (
                    tweet_id, token_name, token_symbol, contract_address, chain,
                    clanker_url, requester_username, requester_user_id, requester_name,
                    fee_recipient_username, fee_recipient_address, deployed_at, raw_tweet_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tweet_id,
                token_name,
                kwargs.get("token_symbol"),
                contract_address,
                kwargs.get("chain", "base"),
                kwargs.get("clanker_url"),
                requester_username,
                kwargs.get("requester_user_id"),
                kwargs.get("requester_name"),
                kwargs.get("fee_recipient_username"),
                kwargs.get("fee_recipient_address"),
                kwargs.get("deployed_at"),
                kwargs.get("raw_tweet_text")
            ))
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            print(f"[DB ERROR] Insert deployment failed: {e}")
            return None
    
    def get_all_deployments(self) -> List[Dict[str, Any]]:
        """Get all deployments."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM deployments ORDER BY deployed_at DESC")
        return [dict(row) for row in cursor.fetchall()]
    
    def get_deployments_by_requester(self, username: str) -> List[Dict[str, Any]]:
        """Get deployments by requester username."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM deployments WHERE requester_username = ? ORDER BY deployed_at DESC",
            (username,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_deployment_by_contract(self, contract: str) -> Optional[Dict[str, Any]]:
        """Get deployment by contract address."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM deployments WHERE LOWER(contract_address) = LOWER(?)",
            (contract,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_tweet_count(self) -> int:
        """Get total tweet count."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tweets")
        return cursor.fetchone()[0]
    
    def get_deployment_count(self) -> int:
        """Get total deployment count."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM deployments")
        return cursor.fetchone()[0]
    
    def get_unique_deployers(self) -> List[str]:
        """Get list of unique deployer usernames."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT requester_username 
            FROM deployments 
            WHERE requester_username IS NOT NULL
            ORDER BY requester_username
        """)
        return [row[0] for row in cursor.fetchall()]
    
    def get_deployer_stats(self) -> List[Dict[str, Any]]:
        """Get deployment stats per deployer."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                requester_username,
                COUNT(*) as deployment_count,
                MIN(deployed_at) as first_deployment,
                MAX(deployed_at) as last_deployment
            FROM deployments
            WHERE requester_username IS NOT NULL
            GROUP BY requester_username
            ORDER BY deployment_count DESC
        """)
        return [dict(row) for row in cursor.fetchall()]
    
    def get_recent_tweets(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent tweets."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM tweets ORDER BY received_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def tweet_exists(self, tweet_id: str) -> bool:
        """Check if tweet already exists in database."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM tweets WHERE id = ?", (tweet_id,))
        return cursor.fetchone() is not None
    
    # LIKES METHODS
    
    def add_like(self, tweet_id: str, telegram_user_id: str, 
                 telegram_username: Optional[str] = None,
                 telegram_message_id: Optional[int] = None) -> bool:
        """Add a like to a tweet. Returns True if new like, False if already liked."""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO likes (tweet_id, telegram_user_id, telegram_username, telegram_message_id)
                VALUES (?, ?, ?, ?)
            """, (tweet_id, telegram_user_id, telegram_username, telegram_message_id))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Already liked
            return False
    
    def remove_like(self, tweet_id: str, telegram_user_id: str) -> bool:
        """Remove a like from a tweet. Returns True if removed, False if not found."""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM likes WHERE tweet_id = ? AND telegram_user_id = ?
        """, (tweet_id, telegram_user_id))
        conn.commit()
        return cursor.rowcount > 0
    
    def get_like_count(self, tweet_id: str) -> int:
        """Get number of likes for a tweet."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM likes WHERE tweet_id = ?", (tweet_id,))
        return cursor.fetchone()[0]
    
    def has_liked(self, tweet_id: str, telegram_user_id: str) -> bool:
        """Check if user has liked a tweet."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM likes WHERE tweet_id = ? AND telegram_user_id = ?",
            (tweet_id, telegram_user_id)
        )
        return cursor.fetchone() is not None
    
    def get_likes_for_tweet(self, tweet_id: str) -> List[Dict[str, Any]]:
        """Get all likes for a tweet."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM likes WHERE tweet_id = ? ORDER BY liked_at DESC",
            (tweet_id,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_like_usernames(self, tweet_id: str) -> List[str]:
        """Get usernames of users who liked a tweet, in order (first liker first)."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT telegram_username FROM likes WHERE tweet_id = ? ORDER BY liked_at ASC",
            (tweet_id,)
        )
        # Return list of usernames (filter out None/empty)
        return [row[0] for row in cursor.fetchall() if row[0]]
    
    def get_user_likes(self, telegram_user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all tweets liked by a user."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM likes WHERE telegram_user_id = ? ORDER BY liked_at DESC LIMIT ?",
            (telegram_user_id, limit)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    # USER STATS METHODS (Deploys Received)
    
    def increment_deploys_received(self, username: str, count: int = 1) -> int:
        """Increment deploys_received for a user. Returns new count."""
        username = username.lower().lstrip("@")
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO user_stats (username, deploys_received, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(username) DO UPDATE SET 
                deploys_received = deploys_received + ?,
                updated_at = CURRENT_TIMESTAMP
        """, (username, count, count))
        conn.commit()
        
        # Return new count
        cursor.execute("SELECT deploys_received FROM user_stats WHERE username = ?", (username,))
        row = cursor.fetchone()
        return row[0] if row else 0
    
    def get_deploys_received(self, username: str) -> int:
        """Get deploys_received count for a user."""
        username = username.lower().lstrip("@")
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT deploys_received FROM user_stats WHERE username = ?", (username,))
        row = cursor.fetchone()
        return row[0] if row else 0
    
    def increment_deploys_made(self, username: str, count: int = 1) -> int:
        """Increment deploys_made counter for a user (FROM - who requested deploy)."""
        username = username.lower().lstrip("@")
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO user_stats (username, deploys_made, deploys_received, updated_at)
            VALUES (?, ?, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(username) DO UPDATE SET 
                deploys_made = deploys_made + ?,
                updated_at = CURRENT_TIMESTAMP
        """, (username, count, count))
        conn.commit()
        
        # Return new count
        cursor.execute("SELECT deploys_made FROM user_stats WHERE username = ?", (username,))
        row = cursor.fetchone()
        return row[0] if row else 0
    
    def get_deploys_made(self, username: str) -> int:
        """Get deploys_made count for a user."""
        username = username.lower().lstrip("@")
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT deploys_made FROM user_stats WHERE username = ?", (username,))
        row = cursor.fetchone()
        return row[0] if row else 0
    
    def get_all_user_stats(self, order_by: str = "deploys_received") -> List[Dict[str, Any]]:
        """Get all user stats ordered by specified column."""
        conn = self.connect()
        cursor = conn.cursor()
        if order_by == "deploys_made":
            cursor.execute("SELECT * FROM user_stats ORDER BY deploys_made DESC")
        else:
            cursor.execute("SELECT * FROM user_stats ORDER BY deploys_received DESC")
        return [dict(row) for row in cursor.fetchall()]
    
    # BANKR TWEETS METHODS (Backfill)
    
    def insert_bankr_tweet(self, tweet: Dict[str, Any]) -> bool:
        """Insert a bankr tweet for backfill processing."""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO bankr_tweets (
                    id, text, created_at, in_reply_to_id, in_reply_to_username, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                tweet.get("id"),
                tweet.get("text"),
                tweet.get("createdAt"),
                tweet.get("inReplyToId"),
                tweet.get("inReplyToUsername"),
                json.dumps(tweet, ensure_ascii=False)
            ))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"[DB ERROR] Insert bankr tweet failed: {e}")
            return False
    
    def insert_bankr_tweets_batch(self, tweets: List[Dict[str, Any]]) -> int:
        """Insert multiple bankr tweets. Returns count of new inserts."""
        conn = self.connect()
        cursor = conn.cursor()
        
        inserted = 0
        for tweet in tweets:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO bankr_tweets (
                        id, text, created_at, in_reply_to_id, in_reply_to_username, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    tweet.get("id"),
                    tweet.get("text"),
                    tweet.get("createdAt"),
                    tweet.get("inReplyToId"),
                    tweet.get("inReplyToUsername"),
                    json.dumps(tweet, ensure_ascii=False)
                ))
                if cursor.rowcount > 0:
                    inserted += 1
            except Exception as e:
                print(f"[DB ERROR] Insert bankr tweet {tweet.get('id')} failed: {e}")
        
        conn.commit()
        return inserted
    
    def get_bankr_tweets_count(self) -> int:
        """Get total count of bankr tweets."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM bankr_tweets")
        return cursor.fetchone()[0]
    
    def get_unprocessed_bankr_tweets(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get unprocessed bankr tweets for Agent classification."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM bankr_tweets WHERE processed = 0 ORDER BY created_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def mark_bankr_tweet_processed(self, tweet_id: str, agent_result: str = "", 
                                    fee_recipients: List[str] = None) -> bool:
        """Mark a bankr tweet as processed with Agent result."""
        conn = self.connect()
        cursor = conn.cursor()
        
        fee_str = json.dumps(fee_recipients) if fee_recipients else "[]"
        
        try:
            cursor.execute("""
                UPDATE bankr_tweets SET 
                    processed = 1,
                    agent_result = ?,
                    fee_recipients = ?
                WHERE id = ?
            """, (agent_result, fee_str, tweet_id))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"[DB ERROR] Mark processed failed: {e}")
            return False
    
    def get_backfill_progress(self) -> Dict[str, int]:
        """Get backfill progress stats."""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM bankr_tweets")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM bankr_tweets WHERE processed = 1")
        processed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM user_stats")
        users = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(deploys_received) FROM user_stats")
        row = cursor.fetchone()
        total_received = row[0] if row[0] else 0
        
        cursor.execute("SELECT SUM(deploys_made) FROM user_stats")
        row = cursor.fetchone()
        total_made = row[0] if row[0] else 0
        
        return {
            "total_tweets": total,
            "processed": processed,
            "pending": total - processed,
            "unique_users": users,
            "total_deploys_made": total_made,
            "total_deploys_received": total_received
        }
