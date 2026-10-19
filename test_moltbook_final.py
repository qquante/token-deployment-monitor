"""
Test Moltbook API - Replicando Headers do Browser EXATAMENTE
"""

import asyncio
import aiohttp
import json
from datetime import datetime


async def test_moltbook_api():
    """Test Moltbook API com headers EXATOS do browser."""
    
    url = "https://www.moltbook.com/api/v1/submolts/clawnch?sort=new"
    
    # Headers EXATOS do browser (SEM pseudo-headers HTTP/2!)
    headers = {
        # Accept headers
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,zh;q=0.5",
        
        # Cookies (IMPORTANTES!)
        "cookie": "[REMOVED_CREDENTIAL]",
        
        # Priority
        "priority": "u=1, i",
        
        # Referer
        "referer": "https://www.moltbook.com/m/clawnch",
        
        # Security headers (Chrome 144)
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        
        # User-Agent (Chrome 144)
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    }
    
    print("=" * 80)
    print("🧪 TESTE MOLTBOOK API - HEADERS COMPLETOS")
    print("=" * 80)
    print(f"\n📍 URL: {url}")
    print(f"⏰ Timestamp: {datetime.now().isoformat()}")
    print("\n📤 HEADERS ENVIADOS:")
    print("-" * 80)
    for key, value in headers.items():
        # Truncar valores muito longos para exibição
        display_value = value if len(value) < 100 else value[:97] + "..."
        print(f"  {key}: {display_value}")
    print("-" * 80)
    
    try:
        # Criar sessão com timeout maior
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            print("\n🔄 Enviando request...")
            print(f"⏱️  Timeout: 60s")
            
            start_time = datetime.now()
            
            async with session.get(url, headers=headers) as resp:
                elapsed = (datetime.now() - start_time).total_seconds()
                
                print(f"\n✅ RESPOSTA RECEBIDA! ({elapsed:.2f}s)")
                print("=" * 80)
                print(f"Status: {resp.status} {resp.reason}")
                print(f"Content-Type: {resp.headers.get('content-type', 'N/A')}")
                print(f"Content-Encoding: {resp.headers.get('content-encoding', 'N/A')}")
                print(f"Content-Length: {resp.headers.get('content-length', 'N/A')}")
                print(f"Server: {resp.headers.get('server', 'N/A')}")
                print(f"X-Vercel-Cache: {resp.headers.get('x-vercel-cache', 'N/A')}")
                print(f"X-Vercel-ID: {resp.headers.get('x-vercel-id', 'N/A')}")
                print("-" * 80)
                
                if resp.status == 200:
                    # Parse JSON
                    data = await resp.json()
                    
                    print("\n📦 PAYLOAD RECEBIDO:")
                    print("=" * 80)
                    
                    # Estrutura geral
                    print(f"✅ success: {data.get('success', False)}")
                    
                    if "submolt" in data:
                        submolt = data["submolt"]
                        print(f"\n📋 SUBMOLT INFO:")
                        print(f"   ID: {submolt.get('id', 'N/A')}")
                        print(f"   Name: {submolt.get('name', 'N/A')}")
                        print(f"   Display Name: {submolt.get('display_name', 'N/A')}")
                        print(f"   Subscribers: {submolt.get('subscriber_count', 0)}")
                        print(f"   Description: {submolt.get('description', 'N/A')[:100]}...")
                    
                    if "posts" in data:
                        posts = data["posts"]
                        print(f"\n📝 POSTS: {len(posts)} encontrados")
                        print("-" * 80)
                        
                        for i, post in enumerate(posts[:5], 1):  # Mostrar primeiros 5
                            print(f"\n[POST #{i}]")
                            print(f"  ID: {post.get('id', 'N/A')}")
                            print(f"  Title: {post.get('title', 'N/A')}")
                            print(f"  Author: {post.get('author', {}).get('name', 'N/A')}")
                            print(f"  Created: {post.get('created_at', 'N/A')}")
                            
                            # Mostrar conteúdo (primeiros 200 chars)
                            content = post.get('content', '')
                            if content:
                                print(f"  Content: {content[:200]}...")
                            
                            # Checar se tem !clawnch
                            if content and '!clawnch' in content:
                                print("  ✅ HAS !clawnch TAG!")
                                
                                # Tentar extrair JSON
                                if '```json' in content:
                                    try:
                                        start = content.index('```json') + 7
                                        end = content.index('```', start)
                                        json_str = content[start:end].strip()
                                        token_data = json.loads(json_str)
                                        
                                        print(f"  🪙 TOKEN DATA:")
                                        print(f"     Name: {token_data.get('name', 'N/A')}")
                                        print(f"     Symbol: {token_data.get('symbol', 'N/A')}")
                                        print(f"     Wallet: {token_data.get('wallet', 'N/A')}")
                                        print(f"     Description: {token_data.get('description', 'N/A')[:80]}...")
                                    except Exception as e:
                                        print(f"  ⚠️ Failed to parse JSON: {e}")
                            
                            print("-" * 80)
                        
                        if len(posts) > 5:
                            print(f"\n... e mais {len(posts) - 5} posts")
                    
                    # Salvar payload completo
                    filename = f"moltbook_payload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    
                    print(f"\n💾 Payload completo salvo em: {filename}")
                
                elif resp.status == 404:
                    text = await resp.text()
                    print(f"\n❌ 404 NOT FOUND")
                    print(f"Response body: {text[:500]}")
                
                else:
                    text = await resp.text()
                    print(f"\n⚠️ Status {resp.status}")
                    print(f"Response body: {text[:500]}")
    
    except asyncio.TimeoutError:
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n❌ TIMEOUT após {elapsed:.2f}s")
        print("⚠️ Servidor não respondeu a tempo")
    
    except aiohttp.ClientResponseError as e:
        print(f"\n❌ CLIENT ERROR: {e}")
        print(f"   Status: {e.status}")
        print(f"   Message: {e.message}")
    
    except Exception as e:
        print(f"\n❌ ERRO: {type(e).__name__}")
        print(f"   {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("🏁 TESTE FINALIZADO")
    print("=" * 80)


if __name__ == "__main__":
    print("\n🚀 Iniciando teste da Moltbook API...")
    print("⚙️  Usando headers EXATOS do browser (com cookies!)\n")
    
    asyncio.run(test_moltbook_api())
