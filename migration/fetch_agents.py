import requests
import json
import time
from datetime import datetime
import os
import sys
import urllib.parse
import threading

# Força flush do output
sys.stdout.reconfigure(encoding='utf-8')

# ============== CARREGAR .ENV ==============
def load_env():
    """Carrega variáveis do arquivo .env"""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip()

load_env()

# ============== CONFIGURAÇÕES ==============
MOLTBOOK_API = "https://www.moltbook.com/api/v1/agents/recent"
TWITTER_SEARCH_API = "https://api.twitterapi.io/twitter/tweet/advanced_search"
TWITTER_USER_INFO_API = "https://api.twitterapi.io/twitter/user/info"
TWITTER_USER_ABOUT_API = "https://api.twitterapi.io/twitter/user_about"

TWITTER_API_KEY = os.environ.get("TWITTER_API_KEY", "[REMOVED_CREDENTIAL]")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

MOLTBOOK_INTERVAL_MINUTES = 5
TWITTER_INTERVAL_MINUTES = 1
OUTPUT_DIR = "data"
SEEN_MOLTBOOK_FILE = "data/seen_moltbook.json"
SEEN_TWITTER_FILE = "data/seen_twitter.json"

MOLTBOOK_HEADERS = {
    "accept": "application/json",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": "https://www.moltbook.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
}

TWITTER_HEADERS = {"X-API-Key": TWITTER_API_KEY}


# ============== UTILIDADES ==============
def load_seen(filepath):
    try:
        with open(filepath, "r") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def save_seen(filepath, seen):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(list(seen), f)


def format_number(n):
    try:
        return f"{int(n):,}".replace(",", ".")
    except:
        return str(n)


def parse_twitter_date(date_str):
    """Converte data do Twitter para formato legível."""
    try:
        # Formato: "Thu Apr 16 15:53:45 +0000 2009"
        dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")
        return dt.strftime("%d/%m/%Y")
    except:
        try:
            # Formato ISO
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00").replace(".000000", ""))
            return dt.strftime("%d/%m/%Y")
        except:
            return "N/A"


def get_verification_badge(twitter_data):
    """Retorna badge de verificação."""
    is_blue = twitter_data.get("isBlueVerified", False)
    
    # Verifica badge de empresa
    affiliate_label = twitter_data.get("affiliates_highlighted_label", {}).get("label", {})
    identity_label = twitter_data.get("identity_profile_labels_highlighted_label", {}).get("label", {})
    badge_desc = affiliate_label.get("description") or identity_label.get("description")
    
    if badge_desc:
        return f"☑️ `{badge_desc}`"
    elif is_blue:
        return "✅"
    return ""


def send_telegram(message, preview_url=None):
    """Envia mensagem para o Telegram com preview de link."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[SKIP] Telegram não configurado - crie arquivo .env")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False  # Habilita preview do link
    }
    
    # Se tiver URL específica para preview, usa link_preview_options
    if preview_url:
        payload["link_preview_options"] = {
            "url": preview_url,
            "prefer_small_media": False,
            "show_above_text": False
        }
    
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.ok:
            print("[OK] Telegram enviado!")
            return True
        else:
            print(f"[ERRO] Telegram: {resp.text}")
            return False
    except Exception as e:
        print(f"[ERRO] Telegram: {e}")
        return False


# ============== MOLTBOOK ==============
def fetch_moltbook():
    try:
        response = requests.get(MOLTBOOK_API, headers=MOLTBOOK_HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"[MOLTBOOK] Recebidos {len(data.get('agents', []))} agentes")
        return data
    except Exception as e:
        print(f"[ERRO] Moltbook: {e}")
        return None


def fetch_twitter_user_info(username):
    """Busca dados completos do usuário no Twitter."""
    try:
        # Info básica
        r1 = requests.get(f"{TWITTER_USER_INFO_API}?userName={username}", 
                         headers=TWITTER_HEADERS, timeout=15)
        info = r1.json().get("data", {}) if r1.ok else {}
        
        # Info about (afiliação/badge)
        r2 = requests.get(f"{TWITTER_USER_ABOUT_API}?userName={username}",
                         headers=TWITTER_HEADERS, timeout=15)
        about = r2.json().get("data", {}) if r2.ok else {}
        
        return {**info, **about}
    except Exception as e:
        print(f"[WARN] Twitter user info erro @{username}: {e}")
        return {}


def format_moltbook_message(agent, twitter_data):
    """Formata mensagem do Moltbook."""
    name = agent.get("name", "???")
    description = agent.get("description", "Sem descrição")
    karma = agent.get("karma", 0)
    created_at = agent.get("created_at", "")
    
    owner = agent.get("owner", {})
    x_handle = owner.get("x_handle", "???")
    x_name = owner.get("x_name", x_handle)
    
    # Dados do Twitter (enriquecidos)
    followers = twitter_data.get("followers", owner.get("x_follower_count", 0))
    following = twitter_data.get("following", "?")
    posts = twitter_data.get("statusesCount", "?")
    account_created = parse_twitter_date(twitter_data.get("createdAt", ""))
    
    # Badge de verificação
    badge = get_verification_badge(twitter_data)
    handle_link = f"[@{x_handle}](https://twitter.com/{x_handle})"
    
    if badge:
        owner_line = f"*{x_name}* {badge} ({handle_link})"
    else:
        owner_line = f"*{x_name}* ({handle_link})"
    
    # Data de registro
    try:
        agent_dt = datetime.fromisoformat(created_at.replace("+00:00", ""))
        agent_date = agent_dt.strftime("%d/%m/%Y às %H:%M")
    except:
        agent_date = "N/A"
    
    msg = f"""🆕 *NOVO AGENTE NO MOLTBOOK*

🤖 *Nome:* `{name}`
📝 _{description}_
⭐ *Karma:* `{karma}`

━━━━━━━━━━━━━━━━━━━━━━
👤 *Owner:* {owner_line}

┌ 👥 *Seguidores:* `{format_number(followers)}`
├ 👁️ *Seguindo:* `{format_number(following)}`
├ 📝 *Posts:* `{format_number(posts)}`
└ 📅 *Conta criada:* `{account_created}`
━━━━━━━━━━━━━━━━━━━━━━

⏰ *Registrado em:* `{agent_date}`
🌐 [Ver no Moltbook](https://www.moltbook.com/)

👤 https://twitter.com/{x_handle}"""

    return msg, f"https://twitter.com/{x_handle}"


def process_moltbook(seen):
    """Processa novos agentes do Moltbook."""
    data = fetch_moltbook()
    if not data or not data.get("agents"):
        return seen
    
    # Salvar dados
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, "latest_moltbook.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    new_agents = []
    for agent in data["agents"]:
        agent_id = agent.get("id")
        if agent_id and agent_id not in seen:
            new_agents.append(agent)
            seen.add(agent_id)
    
    if not new_agents:
        print("[MOLTBOOK] Nenhum agente novo")
        return seen
    
    print(f"[MOLTBOOK] {len(new_agents)} novo(s)!")
    
    for agent in new_agents:
        owner = agent.get("owner", {})
        x_handle = owner.get("x_handle", "")
        
        print(f"  -> {agent.get('name')} (@{x_handle})")
        
        # Buscar dados do Twitter
        twitter_data = fetch_twitter_user_info(x_handle) if x_handle else {}
        time.sleep(0.3)
        
        # Formatar e enviar
        message, preview_url = format_moltbook_message(agent, twitter_data)
        send_telegram(message, preview_url)
        
        # Preview no console
        print(message.replace("*", "").replace("`", "").replace("_", "")[:500])
        print()
        
        time.sleep(1)
    
    return seen


# ============== TWITTER SEARCH ==============
def fetch_twitter_search():
    """Busca tweets de verificação do Moltbook."""
    try:
        query = '"on @moltbook" "Verification"'
        encoded = urllib.parse.quote(query)
        url = f"{TWITTER_SEARCH_API}?query={encoded}"
        
        r = requests.get(url, headers=TWITTER_HEADERS, timeout=30)
        data = r.json()
        
        tweets = data.get("tweets", [])
        print(f"[TWITTER] Encontrados {len(tweets)} tweets")
        return tweets
    except Exception as e:
        print(f"[ERRO] Twitter search: {e}")
        return []


def format_twitter_message(tweet):
    """Formata mensagem do tweet de verificação."""
    text = tweet.get("text", "")
    url = tweet.get("url", "")
    created_at = tweet.get("createdAt", "")
    
    author = tweet.get("author", {})
    username = author.get("userName", "???")
    name = author.get("name", username)
    followers = author.get("followers", 0)
    following = author.get("following", 0)
    posts = author.get("statusesCount", 0)
    account_created = parse_twitter_date(author.get("createdAt", ""))
    is_blue = author.get("isBlueVerified", False)
    
    # Badge
    badge = "✅" if is_blue else ""
    handle_link = f"[@{username}](https://twitter.com/{username})"
    
    if badge:
        owner_line = f"*{name}* {badge} ({handle_link})"
    else:
        owner_line = f"*{name}* ({handle_link})"
    
    # Extrair nome do agente do texto
    agent_name = "???"
    if '"' in text:
        parts = text.split('"')
        if len(parts) >= 2:
            agent_name = parts[1]
    
    # Data do tweet
    tweet_date = parse_twitter_date(created_at)
    
    msg = f"""🐦 *TWEET DE VERIFICAÇÃO*

🤖 *Agente:* `{agent_name}`
💬 _{text[:150]}{'...' if len(text) > 150 else ''}_

━━━━━━━━━━━━━━━━━━━━━━
👤 *Autor:* {owner_line}

┌ 👥 *Seguidores:* `{format_number(followers)}`
├ 👁️ *Seguindo:* `{format_number(following)}`
├ 📝 *Posts:* `{format_number(posts)}`
└ 📅 *Conta criada:* `{account_created}`
━━━━━━━━━━━━━━━━━━━━━━

🔗 [Ver tweet]({url})

👤 https://twitter.com/{username}"""

    return msg, f"https://twitter.com/{username}"


def process_twitter(seen):
    """Processa novos tweets de verificação."""
    tweets = fetch_twitter_search()
    
    new_tweets = []
    for tweet in tweets:
        tweet_id = tweet.get("id")
        if tweet_id and tweet_id not in seen:
            new_tweets.append(tweet)
            seen.add(tweet_id)
    
    if not new_tweets:
        print("[TWITTER] Nenhum tweet novo")
        return seen
    
    print(f"[TWITTER] {len(new_tweets)} tweet(s) novo(s)!")
    
    for tweet in new_tweets:
        author = tweet.get("author", {})
        print(f"  -> @{author.get('userName')}: {tweet.get('text', '')[:50]}...")
        
        message, preview_url = format_twitter_message(tweet)
        send_telegram(message, preview_url)
        
        # Preview
        print(message.replace("*", "").replace("`", "").replace("_", "")[:400])
        print()
        
        time.sleep(1)
    
    return seen


# ============== LOOPS ==============
def moltbook_loop():
    """Loop do Moltbook (a cada 5 min)."""
    seen = load_seen(SEEN_MOLTBOOK_FILE)
    print(f"[MOLTBOOK] {len(seen)} agentes já conhecidos")
    
    while True:
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"\n[{timestamp}] [MOLTBOOK] Verificando...")
            
            seen = process_moltbook(seen)
            save_seen(SEEN_MOLTBOOK_FILE, seen)
            
        except Exception as e:
            print(f"[ERRO] Moltbook loop: {e}")
        
        time.sleep(MOLTBOOK_INTERVAL_MINUTES * 60)


def twitter_loop():
    """Loop do Twitter (a cada 1 min)."""
    seen = load_seen(SEEN_TWITTER_FILE)
    print(f"[TWITTER] {len(seen)} tweets já conhecidos")
    
    while True:
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"\n[{timestamp}] [TWITTER] Verificando...")
            
            seen = process_twitter(seen)
            save_seen(SEEN_TWITTER_FILE, seen)
            
        except Exception as e:
            print(f"[ERRO] Twitter loop: {e}")
        
        time.sleep(TWITTER_INTERVAL_MINUTES * 60)


def main():
    print("=" * 50)
    print("🤖 MOLTBOOK + TWITTER TRACKER")
    print("=" * 50)
    print(f"Moltbook: a cada {MOLTBOOK_INTERVAL_MINUTES} min")
    print(f"Twitter: a cada {TWITTER_INTERVAL_MINUTES} min")
    telegram_ok = TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
    print(f"Telegram: {'✅ Configurado' if telegram_ok else '❌ Crie arquivo .env'}")
    print("=" * 50)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Rodar os dois loops em paralelo
    t1 = threading.Thread(target=moltbook_loop, daemon=True)
    t2 = threading.Thread(target=twitter_loop, daemon=True)
    
    t1.start()
    t2.start()
    
    # Manter vivo
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n[STOP] Encerrado pelo usuário")


if __name__ == "__main__":
    main()
