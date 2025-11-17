import os
import re
import random
import asyncio
from datetime import datetime, timedelta
import discord
from discord.ext import commands, tasks
from discord.ui import View, Select, Button
from io import BytesIO
import openpyxl
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

# --- NEW IMPORTS FOR POSTGRESQL/SQLALCHEMY ---
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
# ---------------------------------------------

# Optional PNG export (pure-Pillow)
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False

# -------------------------
# 🔑 Tokens & Logo
# -------------------------
# 🚨 SECURITY FIX: Using environment variable for token
TOKEN = os.getenv('DISCORD_TOKEN')
LOGO_URL = "https://cdn.discordapp.com/attachments/993192595796787200/1413250033545121802/vox_logo.png"

# -------------------------
# ⚙️ Bot Setup
# -------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.presences = True
bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------
# 📦 Storage
# -------------------------
user_links = {}         # {user_id: "https://x.com/..."}
user_stats = {}         # {user_id: {"registrations": int, "wins": int}}
raffles = {}            # {raffle_name: [user_id, user_id]}
already_picked = set()
always_pick = set()
WINNER_ROLE_NAME = "Winner"

# ---------------------------------------------
# 🗃️ Database helpers (PostgreSQL Engine)
# ---------------------------------------------
# --- START: Updated DB Engine Initialization ---
# Load all database components separately
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# We now build the URL from clean components, avoiding parsing errors
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_ENGINE = create_engine(
    DATABASE_URL,
    # Use StaticPool for non-asyncio database connections in an async environment
    poolclass=StaticPool, 
)
# --- END: Updated DB Engine Initialization ---

def db_init():
    # Use DB_ENGINE.begin() for transactional operations (CREATE TABLE)
    with DB_ENGINE.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS users(
            user_id BIGINT PRIMARY KEY,
            x_link TEXT
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS stats(
            user_id BIGINT PRIMARY KEY,
            registrations INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS always_pick(
            user_id BIGINT PRIMARY KEY
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raffles(
            raffle_name TEXT,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            archived INTEGER DEFAULT 0,
            PRIMARY KEY(raffle_name)
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raffle_winners(
            raffle_name TEXT,
            user_id BIGINT,
            picked_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(raffle_name, user_id),
            FOREIGN KEY(raffle_name) REFERENCES raffles(raffle_name)
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS picks_state(
            user_id BIGINT PRIMARY KEY
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS archive_schedule(
            raffle_name TEXT PRIMARY KEY,
            archive_at TIMESTAMP WITHOUT TIME ZONE
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS blacklist(
            user_id BIGINT PRIMARY KEY
        )"""))
        # COMMIT is handled automatically by the .begin() context manager

def load_state_from_db():
    # Use DB_ENGINE.connect() for read-only operations
    with DB_ENGINE.connect() as conn:
        # Fetch data and populate global dictionaries/sets
        for uid, link in conn.execute(text("SELECT user_id, x_link FROM users")).fetchall():
            user_links[uid] = link or ""
        
        for uid, reg, wins in conn.execute(text("SELECT user_id, registrations, wins FROM stats")).fetchall():
            user_stats[uid] = {"registrations": reg or 0, "wins": wins or 0}
        
        always_pick.clear()
        for (uid,) in conn.execute(text("SELECT user_id FROM always_pick")).fetchall():
            always_pick.add(uid)
            
        raffles.clear()
        for (name,) in conn.execute(text("SELECT raffle_name FROM raffles WHERE archived = 0")).fetchall():
            raffles[name] = []
            
        for name, uid in conn.execute(text("""
            SELECT raffle_name, user_id FROM raffle_winners
            JOIN raffles USING(raffle_name)
            WHERE raffles.archived = 0
        """)).fetchall():
            raffles.setdefault(name, []).append(uid)
            
        already_picked.clear()
        for (uid,) in conn.execute(text("SELECT user_id FROM picks_state")).fetchall():
            already_picked.add(uid)

def db_upsert_user(user_id: int, link: str = ""): # Added optional link parameter for convenience
    # Use DB_ENGINE.begin() for transactional operations (INSERT/UPDATE)
    with DB_ENGINE.begin() as conn:
        conn.execute(text("""
        INSERT INTO users(user_id, x_link) VALUES(:user_id, :link)
        ON CONFLICT(user_id) DO UPDATE SET x_link=EXCLUDED.x_link
        """), {"user_id": user_id, "link": link})
        
        conn.execute(text("""
        INSERT INTO stats(user_id, registrations, wins) VALUES(:user_id, 0, 0) 
        ON CONFLICT(user_id) DO NOTHING
        """), {"user_id": user_id})

def db_update_stat(user_id: int, delta_reg: int = 0, delta_wins: int = 0):
    # --- FIX START: Ensure user exists in 'users' table before updating 'stats' ---
    # This prevents the Foreign Key Violation error
    db_upsert_user(user_id) 
    # --- FIX END ---
    
    with DB_ENGINE.begin() as conn:
        conn.execute(text("""
        INSERT INTO stats(user_id, registrations, wins) VALUES(:user_id, :reg_val, :wins_val)
        ON CONFLICT(user_id) DO UPDATE SET
            registrations = stats.registrations + :delta_reg,
            wins = stats.wins + :delta_wins
        """), {
            "user_id": user_id, 
            "reg_val": max(delta_reg, 0), # Values for initial INSERT
            "wins_val": max(delta_wins, 0),
            "delta_reg": delta_reg,      # Values for UPDATE
            "delta_wins": delta_wins
        })

def db_set_always(user_id: int, add=True):
    with DB_ENGINE.begin() as conn:
        if add:
            conn.execute(text("INSERT INTO always_pick(user_id) VALUES(:user_id) ON CONFLICT DO NOTHING"), {"user_id": user_id})
        else:
            conn.execute(text("DELETE FROM always_pick WHERE user_id=:user_id"), {"user_id": user_id})

def db_set_picked(user_id: int, add=True):
    with DB_ENGINE.begin() as conn:
        if add:
            conn.execute(text("INSERT INTO picks_state(user_id) VALUES(:user_id) ON CONFLICT DO NOTHING"), {"user_id": user_id})
        else:
            conn.execute(text("DELETE FROM picks_state WHERE user_id=:user_id"), {"user_id": user_id})

def db_reset_picks(clear_all=True):
    with DB_ENGINE.begin() as conn:
        conn.execute(text("DELETE FROM picks_state"))

def db_create_raffle(raffle_name: str):
    with DB_ENGINE.begin() as conn:
        conn.execute(text("INSERT INTO raffles(raffle_name, archived) VALUES(:name, 0) ON CONFLICT DO NOTHING"), {"name": raffle_name})

def db_add_winner(raffle_name: str, user_id: int):
    # PostgreSQL uses NOW() or CURRENT_TIMESTAMP for ISO format is best handled by DB
    with DB_ENGINE.begin() as conn:
        conn.execute(text("""
        INSERT INTO raffle_winners(raffle_name, user_id, picked_at) 
        VALUES(:name, :user_id, NOW()) 
        ON CONFLICT DO NOTHING
        """), {"name": raffle_name, "user_id": user_id})

def db_archive_raffle(raffle_name: str):
    with DB_ENGINE.begin() as conn:
        conn.execute(text("UPDATE raffles SET archived = 1 WHERE raffle_name = :name"), {"name": raffle_name})
        conn.execute(text("DELETE FROM archive_schedule WHERE raffle_name=:name"), {"name": raffle_name})

def db_is_archived(raffle_name: str) -> bool:
    with DB_ENGINE.connect() as conn:
        row = conn.execute(text("SELECT archived FROM raffles WHERE raffle_name=:name"), {"name": raffle_name}).fetchone()
        return bool(row and row[0] == 1)

def db_get_active_raffles():
    with DB_ENGINE.connect() as conn:
        return [r[0] for r in conn.execute(text("SELECT raffle_name FROM raffles WHERE archived = 0")).fetchall()]

def db_get_archived_raffles():
    with DB_ENGINE.connect() as conn:
        return [r[0] for r in conn.execute(text("SELECT raffle_name FROM raffles WHERE archived = 1")).fetchall()]

def db_schedule_archive(raffle_name: str, when: datetime):
    # Ensure datetime object is in UTC ISO format for DB storage
    with DB_ENGINE.begin() as conn:
        conn.execute(text("INSERT INTO archive_schedule(raffle_name, archive_at) VALUES(:name, :when) ON CONFLICT (raffle_name) DO UPDATE SET archive_at = EXCLUDED.archive_at"),
                    {"name": raffle_name, "when": when.isoformat()})

def db_get_due_archives(now: datetime):
    # Query uses the ISO formatted string for comparison
    with DB_ENGINE.connect() as conn:
        return [r[0] for r in conn.execute(text("SELECT raffle_name FROM archive_schedule WHERE archive_at <= :now"), {"now": now.isoformat()}).fetchall()]

def db_user_wins(user_id: int):
    with DB_ENGINE.connect() as conn:
        return [row[0] for row in conn.execute(text("""
            SELECT raffle_name FROM raffle_winners
            JOIN raffles USING(raffle_name)
            WHERE user_id=:user_id"""), {"user_id": user_id}).fetchall()]

def is_blacklisted(user_id: int) -> bool:
    with DB_ENGINE.connect() as conn:
        row = conn.execute(text("SELECT 1 FROM blacklist WHERE user_id=:user_id"), {"user_id": user_id}).fetchone()
        return row is not None

# -------------------------
# ⏱️ Background: handle scheduled archives
# -------------------------
@tasks.loop(seconds=30.0)
async def archive_watcher():
    now = datetime.utcnow()
    due = db_get_due_archives(now)
    if not due:
        return
    for name in due:
        db_archive_raffle(name)
        raffles.pop(name, None)

@archive_watcher.before_loop
async def before_archive_watcher():
    await bot.wait_until_ready()

# -------------------------
# 🚀 Events
# -------------------------
@bot.event
async def on_ready():
    db_init()
    load_state_from_db()
    archive_watcher.start()
    print(f"✅ Bot is online as {bot.user}")

# -------------------------
# 📝 Commands
# -------------------------
@bot.command()
async def hello(ctx):
    await ctx.send(f"Hello {ctx.author.mention}! 👋")

@bot.command()
async def register(ctx, link: str = None):
    """Register your X (Twitter) link"""
    if not link:
        await ctx.send("⚠️ Please provide your X profile link. Example: `!register https://x.com/username`")
        return
    pattern = r"^https:\/\/(x\.com|twitter\.com)\/[A-Za-z0-9_]+$"
    if not re.match(pattern, link):
        await ctx.send("❌ Invalid link! Example: `https://x.com/username`")
        return
    if ctx.author.id in user_links:
        await ctx.send(f"⚠️ {ctx.author.mention}, already registered with: {user_links[ctx.author.id]}")
        return
    if is_blacklisted(ctx.author.id):
        await ctx.send("❌ You are blacklisted and cannot register.")
        return

    user_links[ctx.author.id] = link
    db_upsert_user(ctx.author.id, link)
    user_stats.setdefault(ctx.author.id, {"registrations": 0, "wins": 0})
    user_stats[ctx.author.id]["registrations"] += 1
    db_update_stat(ctx.author.id, delta_reg=1)
    await ctx.send(f"✅ {ctx.author.mention}, your X link has been registered: {link}")

@bot.command()
async def unregister(ctx, member: discord.Member = None):
    """
    Unregister yourself, or (if admin) unregister another member.
    """
    target = member or ctx.author

    # Check existence
    with DB_ENGINE.connect() as conn:
        exists = conn.execute(text("SELECT 1 FROM users WHERE user_id = :user_id"), {"user_id": target.id}).fetchone()

    if not exists:
        if target == ctx.author:
            await ctx.send(f"⚠️ {ctx.author.mention}, you are not registered.")
        else:
            await ctx.send(f"⚠️ {target.mention} is not registered.")
        return

    if member and not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ You don’t have permission to unregister other users.")
        return

    # Clear local state
    user_links.pop(target.id, None)
    already_picked.discard(target.id)
    always_pick.discard(target.id)
    user_stats.pop(target.id, None)

    # Clear database records
    with DB_ENGINE.begin() as conn:
        conn.execute(text("DELETE FROM users WHERE user_id = :user_id"), {"user_id": target.id})
        conn.execute(text("DELETE FROM stats WHERE user_id = :user_id"), {"user_id": target.id})
        conn.execute(text("DELETE FROM always_pick WHERE user_id = :user_id"), {"user_id": target.id})
        conn.execute(text("DELETE FROM raffle_winners WHERE user_id = :user_id"), {"user_id": target.id})
        conn.execute(text("DELETE FROM picks_state WHERE user_id = :user_id"), {"user_id": target.id})

    if target == ctx.author:
        await ctx.send(f"✅ {ctx.author.mention}, you have been unregistered successfully.")
    else:
        await ctx.send(f"✅ {target.mention} has been unregistered by {ctx.author.mention}.")

@bot.command()
@commands.has_permissions(administrator=True)
async def blacklist(ctx, member: discord.Member):
    """Blacklist a user (they cannot register or be picked)."""
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass
    except discord.HTTPException:
        pass

    with DB_ENGINE.begin() as conn:
        conn.execute(text("INSERT INTO blacklist(user_id) VALUES(:user_id) ON CONFLICT DO NOTHING"), {"user_id": member.id})

    await ctx.send(f"🚫 {member.mention} has been blacklisted.")

@bot.command()
@commands.has_permissions(administrator=True)
async def unblacklist(ctx, member: discord.Member):
    """Remove a user from the blacklist."""
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass
    except discord.HTTPException:
        pass

    with DB_ENGINE.begin() as conn:
        conn.execute(text("DELETE FROM blacklist WHERE user_id=:user_id"), {"user_id": member.id})

    await ctx.send(f"✅ {member.mention} removed from blacklist.")

@bot.command(name="blacklist_list")
@commands.has_permissions(administrator=True)
async def blacklist_list(ctx):
    """Show all blacklisted users."""
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass
    except discord.HTTPException:
        pass
        
    with DB_ENGINE.connect() as conn:
        rows = conn.execute(text("SELECT user_id FROM blacklist")).fetchall()

    if not rows:
        await ctx.send("✅ No users are currently blacklisted.")
        return

    members = []
    for (uid,) in rows:
        member = ctx.guild.get_member(uid)
        if member:
            members.append(member.mention)
        else:
            members.append(f"<@{uid}> (not in server)")

    embed = discord.Embed(
        title="🚫 Blacklisted Users",
        description="\n".join(members),
        color=discord.Color.dark_red()
    )
    embed.set_footer(text=f"Total: {len(members)} users")

    await ctx.send(embed=embed)

@bot.command()
@commands.has_permissions(administrator=True)
async def list_users(ctx):
    """List registered users by tagging them (split across embeds if too long)."""
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass
    except discord.HTTPException:
        pass
    if not user_links:
        await ctx.send("⚠️ No users registered yet.")
        return

    online, offline = [], []
    for uid in user_links.keys():
        member = ctx.guild.get_member(uid)
        if not member:
            continue
        if member.status != discord.Status.offline:
            online.append(member.mention)
        else:
            offline.append(member.mention)

    def make_embed(batch, title, online_count, offline_count):
        embed = discord.Embed(
            title=title,
            description="\n".join(batch),
            color=discord.Color.green() if "Online" in title else discord.Color.red()
        )
        embed.set_footer(
            text=f"Online: {online_count} | Offline: {len(offline)} | Total: {len(user_links)}"
        )
        return embed

    batch, total_chars, batch_num = [], 0, 1
    for user in online:
        if total_chars + len(user) > 5500:
            await ctx.send(embed=make_embed(batch, f"🟢 Online Users (Batch {batch_num})", len(online), len(offline)))
            batch, total_chars = [], 0
            batch_num += 1
        batch.append(user)
        total_chars += len(user)
    if batch:
        await ctx.send(embed=make_embed(batch, f"🟢 Online Users (Batch {batch_num})", len(online), len(offline)))

    batch, total_chars, batch_num = [], 0, 1
    for user in offline:
        if total_chars + len(user) > 5500:
            await ctx.send(embed=make_embed(batch, f"🔴 Offline Users (Batch {batch_num})", len(online), len(offline)))
            batch, total_chars = [], 0
            batch_num += 1
        batch.append(user)
        total_chars += len(user)
    if batch:
        await ctx.send(embed=make_embed(batch, f"🔴 Offline Users (Batch {batch_num})", len(online), len(offline)))

@bot.command()
@commands.has_permissions(administrator=True)
async def pick(ctx, raffle_name: str, number: int):
    """Pick winners for a space. Always-pick users always win, blacklist excluded."""
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass
    except discord.HTTPException:
        pass
    global already_picked

    priority_winners = [
        m for uid in always_pick
        if (m := ctx.guild.get_member(uid))
        and uid not in already_picked
        and not is_blacklisted(uid)
    ]

    eligible = [
        m for uid in user_links
        if (m := ctx.guild.get_member(uid))
        and uid not in already_picked
        and m.status == discord.Status.online
        and uid not in always_pick
        and not is_blacklisted(uid)
    ]

    winners = []

    if priority_winners:
        winners.extend(priority_winners)
        number -= len(priority_winners)

    if number > 0 and eligible:
        winners.extend(random.sample(eligible, min(number, len(eligible))))

    if not winners:
        await ctx.send("⚠️ No eligible users available for this space.")
        return

    random.shuffle(winners)

    raffles.setdefault(raffle_name, [])
    # Create raffle entry in DB
    db_create_raffle(raffle_name)

    winner_role = discord.utils.get(ctx.guild.roles, name=WINNER_ROLE_NAME)
    if not winner_role:
        # NOTE: Bot must have permission to create/manage roles
        winner_role = await ctx.guild.create_role(name=WINNER_ROLE_NAME)

    table = "```\nS/N | Discord Name          | X Username\n" + "-" * 40 + "\n"
    for idx, w in enumerate(winners, start=1):
        already_picked.add(w.id)
        raffles[raffle_name].append(w.id)
        db_add_winner(raffle_name, w.id)
        db_set_picked(w.id)
        
        # This call now correctly handles users who might have been registered 
        # but did not have a stats record yet (which caused the error).
        db_update_stat(w.id, delta_wins=1) 

        user_stats.setdefault(w.id, {"registrations": 0, "wins": 0})
        user_stats[w.id]["wins"] += 1

        # NOTE: Bot must have permission to manage roles (above the member's highest role)
        await w.add_roles(winner_role)

        x_link = user_links.get(w.id, "")
        username = x_link.split("/")[-1] if x_link else "-"
        table += f"{idx:<3} | {w.display_name:<20} | {username}\n"
    table += "```"

    embed = discord.Embed(
        title=f"🎉 Winners - {raffle_name} (Space)",
        description=table,
        color=discord.Color.gold()
    )

    await ctx.send(embed=embed)

@bot.command()
@commands.has_permissions(administrator=True)
async def reset_picks(ctx):
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass
    except discord.HTTPException:
        pass
    global already_picked
    already_picked.clear()
    db_reset_picks(clear_all=True)
    await ctx.send("✅ Picks reset. All users are available for the next space.")

@bot.command()
@commands.has_permissions(administrator=True)
async def status(ctx):
    """Show bot status"""
    total = len(user_links)
    online = [uid for uid in user_links if (m := ctx.guild.get_member(uid)) and m.status != discord.Status.offline]
    offline = [uid for uid in user_links if uid not in online]

    with DB_ENGINE.connect() as conn:
        total_blacklisted = conn.execute(text("SELECT COUNT(*) FROM blacklist")).fetchone()[0]

    embed = discord.Embed(title="📊 Bot Status", color=discord.Color.green())
    embed.add_field(name="📝 Registered", value=total)
    embed.add_field(name="🟢 Online", value=len(online))
    embed.add_field(name="🔴 Offline", value=len(offline))
    embed.add_field(name="🏆 Already Picked", value=len(already_picked))
    embed.add_field(name="🚫 Blacklisted Users",value=total_blacklisted)
    await ctx.send(embed=embed, reference=ctx.message, mention_author=True)

# -------------------------
# 🆕 Profile Command (+ Show Wins button)
# -------------------------
class ShowWinsView(View):
    def __init__(self, user_id: int):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.add_item(Button(label="Show Wins", style=discord.ButtonStyle.primary, custom_id=f"show_wins:{user_id}"))

    @discord.ui.button(label="Show Wins", style=discord.ButtonStyle.primary)
    async def _dummy(self, *args, **kwargs):
        pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id and not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You can only view your own wins.", ephemeral=True)
            return False
        wins = db_user_wins(self.user_id)
        if wins:
            lines = "\n".join(f"• {name}" for name in wins)
        else:
            lines = "No wins yet."
        embed = discord.Embed(title="🏆 Your Space Wins", description=lines, color=discord.Color.purple())
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return True

@bot.command()
async def profile(ctx, member: discord.Member = None):
    """Show gamified profile"""
    user = member or ctx.author
    link = user_links.get(user.id, "Not registered")
    stats = user_stats.setdefault(user.id, {"registrations": 0, "wins": 0})
    wins = stats["wins"]
    
    status_map = {
        discord.Status.online: "🟢 Online",
        discord.Status.idle: "🌙 Away",
        discord.Status.dnd: "⛔ Do Not Disturb",
        discord.Status.offline: "⚫ Offline",
    }
    status_text = status_map.get(user.status, "❓ Unknown")

    ranks = [
        (0, 5, "🌱 Newbie", "🌱"),
        (6, 10, "🎯 Amateur", "🎯"),
        (11, 15, "🛡️ Experienced", "🛡️"),
        (16, 20, "⚡ Skilled", "⚡"),
        (21, 25, "🔥 Advanced", "🔥"),
        (26, 30, "🏆 Pro", "🏆"),
        (31, float("inf"), "👑 Legend", "👑"),
    ]

    rank_name, rank_emoji, progress = "Unranked", "❔", 0
    for lower, upper, name, emoji in ranks:
        if lower <= wins <= upper:
            rank_name, rank_emoji = name, emoji
            next_threshold = upper if upper != float("inf") else None
            progress = 10 if not next_threshold else int((wins - lower) / (upper - lower + 1) * 10)
            break

    bar = "■" * progress + "□" * (10 - progress)
    percent = int(progress * 10)

    embed = discord.Embed(color=discord.Color.blue())
    embed.set_author(name=f"{user.display_name}'s Profile", icon_url=user.display_avatar.url)
    embed.description = (
        f"👤 Profile: {user.mention}\n"
        f"🌐 X Link: {link}\n"
        f"🖥️ Status: {status_text}\n\n"
        f"📊 Stats:\n"
        f"- 📝 Registrations: {stats['registrations']}\n"
        f"- 🏆 Wins: {wins}\n"
        f"- 🎖️ Rank: {rank_emoji} {rank_name}\n"
        f"- 🔋 Progress: [{bar}] {percent}%\n"
    )

    view = ShowWinsView(user.id)
    await ctx.send(embed=embed, view=view, reference=ctx.message, mention_author=True)

# -------------------------
# 🎯 Always-pick
# -------------------------
@bot.command()
@commands.has_permissions(administrator=True)
async def always_add(ctx, member: discord.Member):
    always_pick.add(member.id)
    db_set_always(member.id, add=True)
    await ctx.send(f"✅ {member.mention} added to always-pick list.")

@bot.command()
@commands.has_permissions(administrator=True)
async def always_remove(ctx, member: discord.Member):
    always_pick.discard(member.id)
    db_set_always(member.id, add=False)
    await ctx.send(f"❌ {member.mention} removed.")

@bot.command()
@commands.has_permissions(administrator=True)
async def always_list(ctx):
    if not always_pick:
        await ctx.send("⚠️ No users in always-pick list.")
        return
    members = [ctx.guild.get_member(uid).mention for uid in always_pick if ctx.guild.get_member(uid)]
    await ctx.send("👑 Always-pick list:\n" + ", ".join(members))

# -------------------------
# 📤 Export helpers
# -------------------------
def build_rows_for_raffle(guild: discord.Guild, raffle_name: str):
    rows = [["S/N", "Discord Name", "X Username", "X Link"]]
    winners = []
    if raffle_name in raffles:
        winners = raffles.get(raffle_name, [])
    else:
        # Use DB_ENGINE.connect() for a specific select operation
        with DB_ENGINE.connect() as conn:
            winners = [r[0] for r in conn.execute(text(
                "SELECT user_id FROM raffle_winners WHERE raffle_name=:name ORDER BY picked_at ASC"), {"name": raffle_name}
            ).fetchall()]

    for sn, uid in enumerate(winners, start=1):
        member = guild.get_member(uid)
        display = member.display_name if member else f"User {uid}"
        x_link = user_links.get(uid, "")
        username = x_link.split("/")[-1] if x_link else "-"
        rows.append([sn, display, username, x_link])
    return rows

def export_excel(guild: discord.Guild, raffle_name: str) -> str:
    rows = build_rows_for_raffle(guild, raffle_name)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = raffle_name[:31] if raffle_name else "Sheet1"
    for r in rows:
        ws.append(r)
    file_path = f"{raffle_name}_winners.xlsx"
    wb.save(file_path)
    return file_path

def export_pdf(guild: discord.Guild, raffle_name: str) -> str:
    rows = build_rows_for_raffle(guild, raffle_name)
    file_path = f"{raffle_name}_winners.pdf"
    doc = SimpleDocTemplate(file_path, pagesize=A4, title=f"{raffle_name} Winners")
    styles = getSampleStyleSheet()
    elements = []
    elements.append(Paragraph(f"🎉 Winners - {raffle_name} (Space)", styles['Title']))
    elements.append(Spacer(1, 12))
    table = Table(rows, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.gold),
        ('TEXTCOLOR', (0,0), (-1,0), colors.black),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.whitesmoke, colors.lightgrey])
    ]))
    elements.append(table)
    doc.build(elements)
    return file_path

def export_png(guild: discord.Guild, raffle_name: str) -> str:
    if not PIL_AVAILABLE:
        raise RuntimeError("Pillow not installed; PNG export unavailable.")
    rows = build_rows_for_raffle(guild, raffle_name)
    col_widths = [60, 260, 200, 420]
    padding = 12
    row_h = 36
    width = sum(col_widths) + padding*2
    height = row_h * (len(rows)+1) + padding*2
    img = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("arial.ttf", 18)
        font_b = ImageFont.truetype("arial.ttf", 20)
    except Exception:
        font = ImageFont.load_default()
        font_b = font
    y = padding
    draw.text((padding, y), f"Winners - {raffle_name} (Space)", font=font_b, fill="black")
    y += row_h
    x = padding
    draw.rectangle([x, y, width-padding, y+row_h], outline="black", fill="#ffd166")
    headers = rows[0]
    cx = x
    for i, head in enumerate(headers):
        draw.text((cx+8, y+8), str(head), font=font_b, fill="black")
        cx += col_widths[i]
        draw.line([(cx, y), (cx, y+row_h)], fill="black", width=1)
    draw.rectangle([x, y, width-padding, y+row_h], outline="black", width=1)
    y += row_h
    for r in rows[1:]:
        cx = x
        for i, cell in enumerate(r):
            draw.text((cx+8, y+8), str(cell), font=font, fill="black")
            cx += col_widths[i]
            draw.line([(cx, y), (cx, y+row_h)], fill="#888888", width=1)
        draw.rectangle([x, y, width-padding, y+row_h], outline="#888888", width=1)
        y += row_h
    file_path = f"{raffle_name}_winners.png"
    img.save(file_path)
    return file_path

# -------------------------
# 📦 Export UI
# -------------------------
async def schedule_archive_in_5(raffle_name: str):
    when = datetime.utcnow() + timedelta(minutes=5)
    db_schedule_archive(raffle_name, when)

class ExportButtons(View):
    def __init__(self, raffle_name: str, guild: discord.Guild, archived: bool = False):
        super().__init__(timeout=180)
        self.raffle_name = raffle_name
        self.guild = guild
        self.archived = archived

    @discord.ui.button(label="Export PNG (visible to all)", style=discord.ButtonStyle.secondary, custom_id="exp_png")
    async def _png(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_export(interaction, "png")

    @discord.ui.button(label="Export EXCEL (admin only)", style=discord.ButtonStyle.success, custom_id="exp_xlsx")
    async def _xlsx(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Only admins can export Excel.", ephemeral=True)
            return
        await self._handle_export(interaction, "xlsx")

    @discord.ui.button(label="Export PDF (visible to all)", style=discord.ButtonStyle.danger, custom_id="exp_pdf")
    async def _pdf(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_export(interaction, "pdf")

    async def _handle_export(self, interaction: discord.Interaction, fmt: str):
        has_winners = False
        if self.raffle_name in raffles:
            has_winners = len(raffles.get(self.raffle_name, [])) > 0
        else:
            with DB_ENGINE.connect() as conn:
                row = conn.execute(text("SELECT COUNT(*) FROM raffle_winners WHERE raffle_name=:name"), {"name": self.raffle_name}).fetchone()
                has_winners = row and row[0] > 0

        if not has_winners:
            await interaction.response.send_message(f"⚠️ No winners found in **{self.raffle_name}**.", ephemeral=True)
            return

        try:
            if fmt == "xlsx":
                path = export_excel(self.guild, self.raffle_name)
                await interaction.response.send_message(file=discord.File(path), ephemeral=True)
            elif fmt == "pdf":
                path = export_pdf(self.guild, self.raffle_name)
                await interaction.channel.send(file=discord.File(path), content=f"📄 PDF export for **{self.raffle_name}** (Space)")
                await interaction.response.send_message("✅ PDF exported.", ephemeral=True)
            elif fmt == "png":
                path = export_png(self.guild, self.raffle_name)
                await interaction.channel.send(file=discord.File(path), content=f"🖼️ PNG export for **{self.raffle_name}** (Space)")
                await interaction.response.send_message("✅ PNG exported.", ephemeral=True)
            else:
                await interaction.response.send_message("Unknown export format.", ephemeral=True)
                return
        finally:
            try:
                os.remove(path)
            except Exception:
                pass

        await schedule_archive_in_5(self.raffle_name)

class RaffleSelect(Select):
    def __init__(self, options, archived: bool = False):
        placeholder = "Select a space to export..." if not archived else "Select an archived space..."
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, options=options)
        self.archived = archived

    async def callback(self, interaction: discord.Interaction):
        raffle_name = self.values[0]
        has_any = False
        if not self.archived:
            winners = raffles.get(raffle_name, [])
            has_any = bool(winners)
        else:
            with DB_ENGINE.connect() as conn:
                row = conn.execute(text("SELECT COUNT(*) FROM raffle_winners WHERE raffle_name=:name"), {"name": raffle_name}).fetchone()
                has_any = row and row[0] > 0

        if not has_any:
            await interaction.response.send_message(f"⚠️ No winners found in **{raffle_name}**.", ephemeral=True)
            return

        view = ExportButtons(raffle_name, interaction.guild, archived=self.archived)
        title = "📤 Export options for archived space" if self.archived else "📤 Export options for space"
        await interaction.response.send_message(f"{title} **{raffle_name}**:", view=view, ephemeral=True)

class RaffleDropdown(View):
    def __init__(self, archived: bool = False):
        super().__init__(timeout=180)
        if not archived:
            names = db_get_active_raffles()
        else:
            names = db_get_archived_raffles()
        if not names:
            opts = [discord.SelectOption(label="(none)", description="No spaces available", default=True)]
        else:
            opts = [discord.SelectOption(label=name, description=("Export " + name)) for name in names]
        self.add_item(RaffleSelect(opts, archived=archived))

# -------------------------
# 📤 Commands: Export & Archive
# -------------------------
@bot.command()
async def export(ctx):
    """
    Show export UI for active spaces.
    """
    active = db_get_active_raffles()
    if not active:
        await ctx.send("⚠️ No active spaces available.")
        return
    embed = discord.Embed(
        title="📤 Export Winners (Spaces)",
        description="Select a **space** to export, then choose **PNG / EXCEL / PDF**.\n"
                    "- PNG/PDF: visible to everyone in the channel.\n"
                    "- Excel: admin-only (you’ll get it ephemerally).",
        color=discord.Color.gold()
    )
    await ctx.send(embed=embed, view=RaffleDropdown(archived=False))

@bot.command(name="archive")
async def list_archived(ctx):
    """
    Show archived spaces and allow exporting them (PNG/PDF for all, Excel admin-only).
    """
    archived = db_get_archived_raffles()
    if not archived:
        await ctx.send("📦 No archived spaces yet.")
        return
    embed = discord.Embed(
        title="📦 Archived Spaces",
        description="Select an **archived space** to export its winners.",
        color=discord.Color.blurple()
    )
    await ctx.send(embed=embed, view=RaffleDropdown(archived=True))

@bot.command()
@commands.has_permissions(administrator=True)
async def reset_db(ctx):
    """⚠️ Clears all tables in the database."""
    # Delete from DB
    with DB_ENGINE.begin() as conn:
        conn.execute(text("DELETE FROM users"))
        conn.execute(text("DELETE FROM raffles"))
        conn.execute(text("DELETE FROM stats"))
        conn.execute(text("DELETE FROM always_pick"))
        conn.execute(text("DELETE FROM raffle_winners"))
        conn.execute(text("DELETE FROM picks_state"))
        conn.execute(text("DELETE FROM archive_schedule"))
        conn.execute(text("DELETE FROM blacklist"))

    global user_links, raffles, user_stats, already_picked, always_pick
    user_links.clear()
    raffles.clear()
    user_stats.clear()
    already_picked.clear()
    always_pick.clear()

    await ctx.send("✅ Database has been reset (all users, raffles, stats cleared).")

@bot.command()
@commands.has_permissions(administrator=True)
async def reset_raffles(ctx):
    """⚠️ Clears raffles and stats, but keeps registered users."""
    # Delete from DB
    with DB_ENGINE.begin() as conn:
        conn.execute(text("DELETE FROM raffles"))
        conn.execute(text("DELETE FROM stats"))
        conn.execute(text("DELETE FROM raffle_winners"))
        conn.execute(text("DELETE FROM picks_state"))
        conn.execute(text("DELETE FROM archive_schedule"))

    global raffles, user_stats, already_picked
    raffles.clear()
    user_stats.clear()
    already_picked.clear()

    await ctx.send("✅ Raffles and stats have been reset. Registered users remain.")

# -------------------------
# 🌐 Web Server for Keep-Alive (FREE TIER ONLY)
# -------------------------
from flask import Flask
from threading import Thread

# Create the Flask app
app = Flask(__name__)

@app.route('/')
def home():
    """A simple endpoint for the external pinger to hit."""
    return "Bot is alive and running!"

def run_flask_server():
    """Start Flask in a separate thread."""
    # Note: We must use 0.0.0.0 and the port specified by Render (usually 8080 or the PORT env var)
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# -------------------------
# ▶️ Run Bot (UPDATED)
# -------------------------

# 1. Start the Flask server in a background thread
t = Thread(target=run_flask_server)
t.start()

# 2. Start the Discord bot in the main thread
bot.run(TOKEN)