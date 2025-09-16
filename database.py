# Arquivo: database.py

from supabase import create_client, Client

SUPABASE_URL = "https://hofqjjpheiaqeqlyevax.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhvZnFqanBoZWlhcWVxbHlldmF4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1Nzk2MDI0MSwiZXhwIjoyMDczNTM2MjQxfQ.O_ivnfH6CDcYPm1ZnRHdbpGWzoo4TeKXf-xJzfA0WIw"

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Erro ao conectar com Supabase: {e}")
    supabase = None

# --- FUNÇÕES LAMONICA ---
def buscar_lamonica_por_trip_number(trip_number: str):
    try:
        return supabase.table('lamonica').select('*').eq('trip_number', trip_number).single().execute().data
    except Exception as e: return None

def deletar_lamonica_por_trip_number(trip_number: str):
    try:
        return supabase.table('lamonica').delete().eq('trip_number', trip_number).execute().data
    except Exception as e: return None

# --- FUNÇÕES ETAS ---
def buscar_etas_por_trip_number(trip_number: str):
    try:
        return supabase.table('etas').select('*').eq('trip_number', trip_number).single().execute().data
    except Exception as e: return None

def atualizar_viagem_etas(trip_number: str, dados_para_atualizar: dict):
    try:
        response = supabase.table('etas').update(dados_para_atualizar).eq('trip_number', trip_number).execute()
        # Se a resposta do Supabase contém dados, a atualização foi bem-sucedida.
        return True if response.data else False
    except Exception as e:
        print(f"❌ Erro ao atualizar viagem em ETAS: {e}")
        return False

# --- FUNÇÕES LOTE ---
def criar_registro_lote(dados_lote: dict):
    try:
        return supabase.table('lote').upsert(dados_lote, on_conflict='trip_number').execute().data
    except Exception as e:
        print(f"❌ Erro ao criar registro no lote: {e}")
        return None

# --- FUNÇÃO DE HISTÓRICO ---
def registrar_historico(dados_historico: dict):
    try:
        return supabase.table('historico_viagens').insert(dados_historico).execute().data
    except Exception as e:
        print(f"❌ Erro ao registrar histórico: {e}")
        return None