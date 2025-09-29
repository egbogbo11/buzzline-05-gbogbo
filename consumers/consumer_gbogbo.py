"""
consumer_gbogbo.py

Consume JSON messages from a live data file with categorization focus.
Store messages in category-specific tables and provide category analytics.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

This consumer implements categorization by:
1. Creating separate tables for each category
2. Routing messages to appropriate category tables
3. Maintaining a master table with all messages
4. Providing category-based analytics

Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import pathlib
import sqlite3
import sys
import time
from datetime import datetime
from typing import Dict, List, Tuple

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Database Management Functions
#####################################

def init_db(db_path: pathlib.Path) -> None:
    """
    Initialize the SQLite database with master table and category tables.
    
    Args:
        db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info(f"Initializing database at {db_path}")
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Create master table for all messages
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS all_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT NOT NULL,
                author TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                category TEXT NOT NULL,
                sentiment REAL NOT NULL,
                keyword_mentioned TEXT,
                message_length INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index on category for efficient filtering
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_category ON all_messages(category)
        """)
        
        # Create category metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS category_stats (
                category TEXT PRIMARY KEY,
                message_count INTEGER DEFAULT 0,
                avg_sentiment REAL DEFAULT 0.0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info("Database initialized with master and metadata tables")


def create_category_table(db_path: pathlib.Path, category: str) -> None:
    """
    Create a dedicated table for a specific category if it doesn't exist.
    
    Args:
        db_path (pathlib.Path): Path to the SQLite database file.
        category (str): Category name for the table.
    """
    # Sanitize category name for table naming
    safe_category = category.replace(' ', '_').replace('-', '_').lower()
    table_name = f"category_{safe_category}"
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT NOT NULL,
                author TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                sentiment REAL NOT NULL,
                keyword_mentioned TEXT,
                message_length INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index on timestamp for time-based queries
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{safe_category}_timestamp ON {table_name}(timestamp)
        """)
        
        conn.commit()
        logger.info(f"Created/verified category table: {table_name}")


def insert_message(message_data: Dict, db_path: pathlib.Path) -> None:
    """
    Insert message into both master table and appropriate category table.
    
    Args:
        message_data (Dict): Processed message data.
        db_path (pathlib.Path): Path to the SQLite database file.
    """
    category = message_data.get('category', 'other')
    
    # Ensure category table exists
    create_category_table(db_path, category)
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Insert into master table
        cursor.execute("""
            INSERT INTO all_messages 
            (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            message_data['message'],
            message_data['author'], 
            message_data['timestamp'],
            message_data['category'],
            message_data['sentiment'],
            message_data['keyword_mentioned'],
            message_data['message_length']
        ))
        
        # Insert into category-specific table
        safe_category = category.replace(' ', '_').replace('-', '_').lower()
        table_name = f"category_{safe_category}"
        
        cursor.execute(f"""
            INSERT INTO {table_name}
            (message, author, timestamp, sentiment, keyword_mentioned, message_length)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            message_data['message'],
            message_data['author'], 
            message_data['timestamp'],
            message_data['sentiment'],
            message_data['keyword_mentioned'],
            message_data['message_length']
        ))
        
        # Update category statistics
        cursor.execute("""
            INSERT OR REPLACE INTO category_stats (category, message_count, avg_sentiment, last_updated)
            VALUES (?, 
                COALESCE((SELECT message_count FROM category_stats WHERE category = ?), 0) + 1,
                (SELECT AVG(sentiment) FROM all_messages WHERE category = ?),
                CURRENT_TIMESTAMP)
        """, (category, category, category))
        
        conn.commit()
        logger.info(f"Inserted message into master table and {table_name}")


def get_category_analytics(db_path: pathlib.Path) -> List[Tuple]:
    """
    Get analytics summary for all categories.
    
    Args:
        db_path (pathlib.Path): Path to the SQLite database file.
        
    Returns:
        List of tuples with category analytics.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                category,
                COUNT(*) as message_count,
                AVG(sentiment) as avg_sentiment,
                COUNT(DISTINCT author) as unique_authors,
                MAX(timestamp) as latest_message
            FROM all_messages 
            GROUP BY category 
            ORDER BY message_count DESC
        """)
        
        return cursor.fetchall()


#####################################
# Message Processing Function
#####################################

def process_message(message: Dict) -> Dict:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (Dict): The JSON message as a dictionary.
        
    Returns:
        Dict: Processed message or None if processing fails.
    """
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category", "other"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned", ""),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message for category '{processed_message['category']}': {processed_message['message'][:50]}...")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


#####################################
# Consume Messages from Live Data File
#####################################

def consume_messages_from_file(live_data_path: pathlib.Path, db_path: pathlib.Path, 
                             interval_secs: int, last_position: int) -> int:
    """
    Consume new messages from a file and process them with categorization.
    Each message is expected to be JSON-formatted.

    Args:
        live_data_path (pathlib.Path): Path to the live data file.
        db_path (pathlib.Path): Path to the SQLite database file.
        interval_secs (int): Interval in seconds to check for new messages.
        last_position (int): Last read position in the file.
        
    Returns:
        int: Updated last position in the file.
    """
    logger.info("Called consume_messages_from_file() with categorization focus:")
    logger.info(f"   {live_data_path=}")
    logger.info(f"   {db_path=}")
    logger.info(f"   {interval_secs=}")
    logger.info(f"   {last_position=}")

    messages_processed = 0
    
    while True:
        try:
            logger.info(f"Reading from live data file at position {last_position}.")
            with open(live_data_path, "r") as file:
                # Move to the last read position
                file.seek(last_position)
                
                for line in file:
                    # If we strip whitespace and there is content
                    if line.strip():
                        try:
                            # Use json.loads to parse the stripped line
                            message = json.loads(line.strip())

                            # Call our process_message function
                            processed_message = process_message(message)

                            # If we have a processed message, insert it into the database
                            if processed_message:
                                insert_message(processed_message, db_path)
                                messages_processed += 1
                                
                                # Log category analytics every 10 messages
                                if messages_processed % 10 == 0:
                                    analytics = get_category_analytics(db_path)
                                    logger.info("=== CATEGORY ANALYTICS ===")
                                    for cat, count, avg_sent, authors, latest in analytics:
                                        logger.info(f"Category '{cat}': {count} messages, "
                                                  f"avg sentiment: {avg_sent:.2f}, "
                                                  f"{authors} unique authors")
                                    logger.info("========================")

                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse JSON line: {line.strip()[:50]}... Error: {e}")
                            continue

                # Update the last position that's been read to the current file position
                last_position = file.tell()

                # Return the last position to be used in the next iteration
                return last_position

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}.")
            sys.exit(10)
        except Exception as e:
            logger.error(f"ERROR: Error reading from live data file: {e}")
            sys.exit(11)

        time.sleep(interval_secs)


#####################################
# Define Main Function
#####################################

def main():
    """
    Main function to run the categorization consumer process.

    Reads configuration, initializes the database with category support, 
    and starts consumption with categorization features.
    """
    logger.info("Starting Categorization Consumer to run continuously.")
    logger.info("This consumer will organize messages by category and provide analytics.")

    logger.info("STEP 1. Read environment variables using config functions.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted existing database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize database with categorization support.")
    try:
        init_db(sqlite_path)
        logger.info("SUCCESS: Database initialized with category tables and analytics support.")
    except Exception as e:
        logger.error(f"ERROR: Failed to create db tables: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and categorizing messages.")
    try:
        last_position = 0
        while True:
            last_position = consume_messages_from_file(
                live_data_path, sqlite_path, interval_secs, last_position
            )
            time.sleep(interval_secs)
            
    except KeyboardInterrupt:
        logger.warning("Categorization Consumer interrupted by user.")
        
        # Display final analytics before shutdown
        try:
            logger.info("=== FINAL CATEGORY ANALYTICS ===")
            analytics = get_category_analytics(sqlite_path)
            for cat, count, avg_sent, authors, latest in analytics:
                logger.info(f"Category '{cat}': {count} total messages, "
                          f"avg sentiment: {avg_sent:.2f}, "
                          f"{authors} unique authors, "
                          f"latest: {latest}")
            logger.info("===============================")
        except Exception as e:
            logger.error(f"Error displaying final analytics: {e}")
            
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Categorization Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()