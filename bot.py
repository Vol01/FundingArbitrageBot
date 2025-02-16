import aiohttp
import asyncio
from datetime import datetime, timedelta
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import os
from dotenv import load_dotenv
import sqlite3
from sqlite3 import Error

# Load environment variables
load_dotenv()

# Get token with debug info
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
print(f"Debug - Environment check:")
print(f"Debug - Token exists: {'Yes' if TELEGRAM_TOKEN else 'No'}")
if TELEGRAM_TOKEN:
    print(f"Debug - Token length: {len(TELEGRAM_TOKEN)}")
    print(f"Debug - Token format valid: {'Yes' if ':' in TELEGRAM_TOKEN else 'No'}")
else:
    raise ValueError("TELEGRAM_TOKEN not found in environment variables!")

def create_connection():
    """Create a database connection"""
    try:
        conn = sqlite3.connect('subscribers.db')
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS subscribers
            (chat_id INTEGER PRIMARY KEY)
        ''')
        conn.commit()
        return conn
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

def load_subscribed_users():
    """Load subscribers from database"""
    conn = create_connection()
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id FROM subscribers")
            users = set(row[0] for row in cursor.fetchall())
            print(f"Loaded {len(users)} users from database")
            return users
        except Error as e:
            print(f"Error loading users: {e}")
            return set()
        finally:
            conn.close()
    return set()

def save_subscribed_users(users):
    """Save subscribers to database"""
    conn = create_connection()
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM subscribers")
            cursor.executemany("INSERT INTO subscribers (chat_id) VALUES (?)",
                             [(user,) for user in users])
            conn.commit()
            print(f"Saved {len(users)} users to database")
        except Error as e:
            print(f"Error saving users: {e}")
        finally:
            conn.close()

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    try:
        chat_id = update.effective_chat.id
        print(f"Received /start command from user {chat_id}")
        
        users = load_subscribed_users()
        users.add(chat_id)
        save_subscribed_users(users)
        
        print(f"User {chat_id} successfully subscribed")
        
        await update.message.reply_text(
            "Hello! I will send you top 5 arbitrage opportunities every hour.\n"
            "Use /stop to unsubscribe from notifications.\n"
            "Use /status to check bot status."
        )
    except Exception as e:
        print(f"Error in start_command: {e}")

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop command"""
    try:
        chat_id = update.effective_chat.id
        print(f"Received /stop command from user {chat_id}")
        
        users = load_subscribed_users()
        users.discard(chat_id)
        save_subscribed_users(users)
        
        print(f"User {chat_id} successfully unsubscribed")
        
        await update.message.reply_text(
            "You have unsubscribed from notifications. Use /start to subscribe again."
        )
    except Exception as e:
        print(f"Error in stop_command: {e}")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command"""
    try:
        chat_id = update.effective_chat.id
        users = load_subscribed_users()
        current_time = datetime.now().strftime('%H:%M:%S')
        
        await update.message.reply_text(
            f"🤖 Bot Status Report\n\n"
            f"Time: {current_time}\n"
            f"Total subscribers: {len(users)}\n"
            f"Your chat ID: {chat_id}\n"
            f"Bot is running! ✅"
        )
    except Exception as e:
        print(f"Error in status_command: {e}")

async def send_telegram_message(message):
    """Send message to all subscribed users"""
    try:
        users = load_subscribed_users()
        if not users:
            print("No subscribed users found!")
            return
            
        print(f"Attempting to send message to {len(users)} users")
        
        async with Application.builder().token(TELEGRAM_TOKEN).build() as app:
            for user_id in users:
                try:
                    await app.bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode='HTML'
                    )
                    print(f"Successfully sent message to user {user_id}")
                except Exception as e:
                    print(f"Error sending to user {user_id}: {e}")
    except Exception as e:
        print(f"Error in send_telegram_message: {e}")

# ... (остальные функции остаются без изменений)

async def run_bot():
    """Main bot function"""
    print("Starting bot...")
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("status", status_command))
    
    print("Bot handlers configured")
    
    async with app:
        print("Bot is running")
        await app.start()
        print("Bot started successfully")
        await run_hourly()
        await app.stop()

if __name__ == "__main__":
    asyncio.run(run_bot())
