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
        print("DEBUG: Database connection created successfully")
        return conn
    except Error as e:
        print(f"ERROR connecting to database: {e}")
        return None

def load_subscribed_users():
    """Load subscribers from database"""
    conn = create_connection()
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id FROM subscribers")
            users = set(row[0] for row in cursor.fetchall())
            print(f"DEBUG: Loaded {len(users)} users from database: {users}")
            return users
        except Error as e:
            print(f"ERROR loading users: {e}")
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
            print(f"DEBUG: Saved {len(users)} users to database: {users}")
        except Error as e:
            print(f"ERROR saving users: {e}")
        finally:
            conn.close()

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    try:
        chat_id = update.effective_chat.id
        print(f"DEBUG: Received /start command from user {chat_id}")
        
        # Load current users
        users = load_subscribed_users()
        print(f"DEBUG: Current subscribers before adding new user: {users}")
        
        # Add new user
        users.add(chat_id)
        print(f"DEBUG: Subscribers after adding new user: {users}")
        
        # Save updated users
        save_subscribed_users(users)
        print(f"DEBUG: Saved subscribers to database")
        
        # Verify save
        verified_users = load_subscribed_users()
        print(f"DEBUG: Verified subscribers in database: {verified_users}")
        
        await update.message.reply_text(
            "Hello! I will send you top 5 arbitrage opportunities every hour.\n"
            "Use /stop to unsubscribe from notifications.\n"
            "Use /status to check bot status."
        )
        print(f"DEBUG: Sent welcome message to user {chat_id}")
        
    except Exception as e:
        print(f"ERROR in start_command: {str(e)}")
        print(f"ERROR type: {type(e)}")

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop command"""
    try:
        chat_id = update.effective_chat.id
        print(f"DEBUG: Received /stop command from user {chat_id}")
        
        users = load_subscribed_users()
        print(f"DEBUG: Current subscribers before removal: {users}")
        
        users.discard(chat_id)
        print(f"DEBUG: Subscribers after removal: {users}")
        
        save_subscribed_users(users)
        print(f"DEBUG: Saved updated subscribers to database")
        
        await update.message.reply_text(
            "You have unsubscribed from notifications. Use /start to subscribe again."
        )
        print(f"DEBUG: Sent unsubscribe message to user {chat_id}")
    except Exception as e:
        print(f"ERROR in stop_command: {str(e)}")

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
        print(f"DEBUG: Sent status to user {chat_id}")
    except Exception as e:
        print(f"ERROR in status_command: {str(e)}")

async def get_hyperliquid_funding(session):
    """Get funding data from Hyperliquid"""
    async def get_meta():
        async with session.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "meta"}
        ) as response:
            data = await response.json()
            return [coin['name'] for coin in data['universe'] if not coin.get('isDelisted', False)]

    async def get_funding_for_coin(coin, start_time, end_time, retries=3):
        for attempt in range(retries):
            try:
                async with session.post(
                    "https://api.hyperliquid.xyz/info",
                    json={"type": "fundingHistory", "coin": coin, "startTime": start_time, "endTime": end_time}
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data and len(data) > 0:
                            latest = data[-1]
                            return {
                                'coin': coin,
                                'funding_rate': float(latest['fundingRate']),
                                'timestamp': latest['time']
                            }
                    await asyncio.sleep(0.1)
            except Exception:
                await asyncio.sleep(0.2 * (attempt + 1))
        return None

    all_coins = await get_meta()
    end_time = int(time.time() * 1000)
    start_time = end_time - (60 * 60 * 1000)
    
    all_results = []
    for i in range(0, len(all_coins), 5):
        batch = all_coins[i:i + 5]
        tasks = [get_funding_for_coin(coin, start_time, end_time) for coin in batch]
        results = await asyncio.gather(*tasks)
        all_results.extend([r for r in results if r is not None])
        await asyncio.sleep(0.2)
    
    return {data['coin']: data['funding_rate'] for data in all_results}

async def get_paradex_funding(session):
    """Get funding data from Paradex"""
    async with session.get("https://api.prod.paradex.trade/v1/markets") as response:
        markets_data = await response.json()
        markets = [market["symbol"] for market in markets_data["results"]]

    all_results = []
    for market in markets:
        try:
            async with session.get(f"https://api.prod.paradex.trade/v1/funding/data?market={market}") as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('results') and len(data['results']) > 0:
                        latest = data['results'][0]
                        ticker = market.split('-')[0]
                        funding_data = {
                            'coin': ticker,
                            'funding_rate': float(latest.get('funding_rate', '0')),
                        }
                        all_results.append(funding_data)
            await asyncio.sleep(0.2)
        except Exception:
            continue
    
    return {data['coin']: data['funding_rate'] for data in all_results}

async def send_telegram_message(message):
    """Send message to all subscribed users"""
    try:
        users = load_subscribed_users()
        print(f"DEBUG: Loaded subscribers for sending message: {users}")
        
        if not users:
            print("WARNING: No subscribed users found!")
            return
            
        print(f"INFO: Attempting to send message to {len(users)} users")
        
        async with Application.builder().token(TELEGRAM_TOKEN).build() as app:
            for user_id in users:
                try:
                    await app.bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode='HTML'
                    )
                    print(f"SUCCESS: Sent message to user {user_id}")
                except Exception as e:
                    print(f"ERROR: Failed to send to user {user_id}: {str(e)}")
    except Exception as e:
        print(f"ERROR in send_telegram_message: {str(e)}")

async def main():
    """Main function to fetch and compare funding rates"""
    try:
        async with aiohttp.ClientSession() as session:
            print("Getting data from Hyperliquid...")
            hyper_data = await get_hyperliquid_funding(session)
            print(f"Received {len(hyper_data)} coins from Hyperliquid")
            
            print("\nGetting data from Paradex...")
            para_data = await get_paradex_funding(session)
            print(f"Received {len(para_data)} coins from Paradex")
            
            common_coins = set(hyper_data.keys()) & set(para_data.keys())
            
            arbitrage_opportunities = []
            for coin in common_coins:
                hyper_rate = hyper_data[coin] * 100
                para_rate = para_data[coin] * 100
                difference = abs(hyper_rate - para_rate)
                
                if hyper_rate > para_rate:
                    hyper_direction = "short"
                    para_direction = "long"
                else:
                    hyper_direction = "long"
                    para_direction = "short"
                
                arbitrage_opportunities.append({
                    'coin': coin,
                    'hyper_rate': hyper_rate,
                    'para_rate': para_rate,
                    'difference': difference,
                    'hyper_direction': hyper_direction,
                    'para_direction': para_direction
                })
            
            arbitrage_opportunities.sort(key=lambda x: x['difference'], reverse=True)
            arbitrage_opportunities = arbitrage_opportunities[:5]
            
            if arbitrage_opportunities:
                current_time = datetime.now().strftime('%H:%M:%S')
                message = f"🔄 Top 5 arbitrage opportunities (time: {current_time}):\n\n"
                for arb in arbitrage_opportunities:
                    message += (f"<b>{arb['coin']}</b>\n"
                              f"Hyperliquid ({arb['hyper_direction']}): {arb['hyper_rate']:+.4f}%\n"
                              f"Paradex ({arb['para_direction']}): {arb['para_rate']:+.4f}%\n"
                              f"Difference: {arb['difference']:.4f}%\n\n")
                
                await send_telegram_message(message)
                print(f"\nSent {len(arbitrage_opportunities)} opportunities")
            else:
                print("\nNo arbitrage opportunities found")
        
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        print(f"Error type: {type(e)}")
        print("\nFull error message:")
        print(str(e))

async def run_hourly():
    """Run the main function hourly"""
    while True:
        await main()
        
        now = datetime.now()
        next_hour = (now.replace(minute=0, second=0, microsecond=0) + 
                    timedelta(hours=1))
        wait_seconds = (next_hour - now).total_seconds()
        print(f"\nNext run in {wait_seconds:.0f} seconds")
        await asyncio.sleep(wait_seconds)

async def run_bot():
    """Main bot function"""
    print("Starting bot...")
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Add command handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("status", status_command))
    
    print("Bot handlers configured")
    
    # Start the bot and run it concurrently with the hourly task
    async with app:
        print("Starting both bot and hourly task...")
        await app.start()
        print("Bot started successfully")
        
        # Create tasks for both the bot polling and hourly updates
        bot_task = app.run_polling(allowed_updates=Update.ALL_TYPES)
        hourly_task = run_hourly()
        
        # Run both tasks concurrently
        try:
            await asyncio.gather(bot_task, hourly_task)
        except Exception as e:
            print(f"Error in main loop: {e}")
        finally:
            print("Stopping bot...")
            await app.stop()
            print("Bot stopped")

if __name__ == "__main__":
    asyncio.run(run_bot())
