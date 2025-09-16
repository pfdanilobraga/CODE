# Arquivo: main.py

from flask import Flask, jsonify, request, render_template
import database
import pandas as pd
from datetime import datetime, timezone
import io

app = Flask(__name__)


@app.route('/')
def homepage():
    return render_template('index.html')


@app.route('/etas')
def etas_page():
    return render_template('etas.html')


# NOVA ROTA PARA A PÁGINA LOTE/MONITORAMENTO
@app.route('/lote')
def lote_page():
    return render_template('lote.html')


# --- ROTAS DA API ---

# NOVA API PARA ADICIONAR NOVA VIAGEM MANUALMENTE
@app.route('/api/nova-viagem', methods=['POST'])
def adicionar_nova_viagem():
    try:
        dados_viagem = request.json
        trip_number = dados_viagem.get('trip_number')
        if not trip_number:
            return jsonify({'erro': 'Trip Number é obrigatório.'}), 400

        # Verifica se o Trip Number já existe nas tabelas etas ou lamonica
        viagem_existente_etas = database.buscar_etas_por_trip_number(trip_number)
        viagem_existente_lamonica = database.buscar_lamonica_por_trip_number(trip_number)
        if viagem_existente_etas or viagem_existente_lamonica:
            return jsonify({'erro': 'Viagem com este Trip Number já existe.'}), 409

        # Define status inicial e data de registro
        dados_viagem['status_agrupado'] = 'ETA ORIGEM'
        dados_viagem['created_at'] = datetime.now(timezone.utc).isoformat()

        # Insere o novo registro na tabela 'etas'
        response = database.supabase.table('etas').insert(dados_viagem).execute()

        if not response.data:
            raise Exception("Falha ao criar o registro da nova viagem no banco de dados.")

        # Registra no histórico
        dados_historico = {
            'trip_number': trip_number,
            'viagem_id': response.data[0]['id'],
            'operador_nome': 'Operador Padrão',
            'acao': 'ADICAO_MANUAL',
            'detalhes': {'status_definido': 'ETA ORIGEM'}
        }
        database.registrar_historico(dados_historico)

        return jsonify({'mensagem': f'Viagem {trip_number} criada com sucesso!'}), 201

    except Exception as e:
        print(f"ERRO AO ADICIONAR NOVA VIAGEM: {e}")
        return jsonify({'erro': str(e)}), 500


# (O resto do main.py completo para evitar confusão)
@app.route('/api/lote', methods=['GET'])
def get_lote_data():
    response = database.supabase.table('lote').select('*').order('trip_number').execute()
    return jsonify(response.data)


@app.route('/api/viagens/<trip_number>/confirmar-cpt', methods=['POST'])
def confirmar_cpt(trip_number):
    try:
        dados_recebidos = request.json
        lacre = dados_recebidos.get('lacre')

        print(f"Tentando atualizar a viagem {trip_number} em ETAS...")
        dados_update_etas = {
            'status_agrupado': 'CPT ORIGEM',
            'cpt_realizado': datetime.now(timezone.utc).isoformat(),
            'lacre': lacre
        }

        sucesso_update = database.atualizar_viagem_etas(trip_number, dados_update_etas)
        if not sucesso_update:
            print(
                f"DEBUG: Falha na atualização. Verifique a tabela 'etas' e a função 'database.atualizar_viagem_etas'.")
            raise Exception("Não foi possível atualizar a viagem em ETAS.")

        print(f"Atualização em ETAS bem-sucedida. Buscando dados completos...")
        viagem_completa = database.buscar_etas_por_trip_number(trip_number)
        if not viagem_completa:
            print(f"DEBUG: Viagem completa não encontrada após a atualização.")
            raise Exception("Dados da viagem não encontrados para criação do registro LOTE.")

        dados_para_lote = {
            'trip_number': viagem_completa.get('trip_number'),
            'lacre': lacre,
            'status': 'EM TRÂNSITO',  # Status inicial no LOTE
            'previsao_chegada': viagem_completa.get('eta_destination_edited'),
            'distancia_faltante': viagem_completa.get('distancia_faltante'),
            'posicao': viagem_completa.get('posicao_atual_descricao'),
            'data_hora_registro': datetime.now(timezone.utc).isoformat()
        }
        print(f"Criando registro na tabela LOTE com os dados: {dados_para_lote}")
        database.criar_registro_lote(dados_para_lote)

        dados_historico = {
            'trip_number': trip_number,
            'viagem_id': viagem_completa.get('id'),
            'operador_nome': 'Operador Padrão',
            'acao': 'CONFIRMACAO_CPT_ORIGEM',
            'detalhes': {'status_definido': 'CPT ORIGEM', 'lacre': lacre}
        }
        database.registrar_historico(dados_historico)

        print(f"Processo de CPT concluído para a viagem {trip_number}.")
        return jsonify({'mensagem': 'CPT confirmado com sucesso!'})

    except Exception as e:
        print(f"ERRO AO CONFIRMAR CPT: {e}")
        return jsonify({'erro': str(e)}), 500


@app.route('/api/lamonica', methods=['GET'])
def get_lamonica_data():
    try:
        viagens_response = database.supabase.table('lamonica').select('*').order('trip_number').execute()
        viagens = viagens_response.data or []
        motoristas_response = database.supabase.table('motoristas_cadastrados').select(
            'driver_id, driver_name').execute()
        motoristas = motoristas_response.data or []
        mapa_motoristas = {str(m['driver_id']): m['driver_name'] for m in motoristas}
        for viagem in viagens:
            driver_id = viagem.get('driver_id')
            if driver_id:
                viagem['driver_name'] = mapa_motoristas.get(str(driver_id), viagem.get('driver_name'))
        return jsonify(viagens)
    except Exception as e:
        return jsonify({'erro': f'Erro ao buscar dados da Lamonica: {str(e)}'}), 500


@app.route('/api/lamonica/<trip_number>', methods=['GET'])
def get_lamonica_trip(trip_number):
    try:
        viagem = database.buscar_lamonica_por_trip_number(trip_number)
        if viagem:
            driver_id = viagem.get('driver_id')
            if driver_id:
                motorista_resp = database.supabase.table('motoristas_cadastrados').select('driver_name').eq('driver_id',
                                                                                                            str(driver_id)).single().execute()
                if motorista_resp.data:
                    viagem['driver_name'] = motorista_resp.data['driver_name']
            return jsonify(viagem)
        return jsonify({'erro': 'Viagem não encontrada'}), 404
    except Exception as e:
        return jsonify({'erro': f'Erro ao buscar viagem: {str(e)}'}), 500


@app.route('/api/importar/motoristas', methods=['POST'])
def importar_motoristas():
    try:
        arquivo_csv = request.files['arquivo']
        content = arquivo_csv.stream.read().decode("utf-8")
        stream = io.StringIO(content)
        df = pd.read_csv(stream)
        df = df.rename(columns={'Driver ID': 'driver_id', 'Driver Name': 'driver_name', 'CPF': 'cpf',
                                'License Plate': 'license_plate', 'Phone Number': 'phone_number',
                                'Vehicle Type': 'vehicle_type', 'City': 'city', 'Status': 'status',
                                'Create Time': 'create_time', 'Modify Time': 'modify_time'})
        colunas_db = ['driver_id', 'driver_name', 'cpf', 'license_plate', 'phone_number', 'vehicle_type', 'city',
                      'status', 'create_time', 'modify_time']
        colunas_existentes = [col for col in colunas_db if col in df.columns]
        df_final = df[colunas_existentes]
        lista_de_registros = df_final.to_dict(orient='records')
        for registro in lista_de_registros:
            for chave, valor in registro.items():
                if pd.isna(valor):
                    registro[chave] = None
                elif isinstance(valor, pd.Timestamp):
                    registro[chave] = valor.isoformat()
                elif chave == 'driver_id' and isinstance(valor, float):
                    registro[chave] = str(int(valor))
        database.supabase.table('motoristas_cadastrados').upsert(lista_de_registros, on_conflict='driver_id').execute()
        return jsonify({'mensagem': f'{len(lista_de_registros)} motoristas importados/atualizados com sucesso!'})
    except Exception as e:
        return jsonify({'erro': f'Ocorreu um erro na importação de motoristas: {str(e)}'}), 500


@app.route('/api/importar/lamonica', methods=['POST'])
def importar_lamonica():
    try:
        arquivo_excel = request.files['arquivo']
        df = pd.read_excel(arquivo_excel, sheet_name='LAMONICA', engine='openpyxl')
        lista_de_registros = df.to_dict(orient='records')
        for registro in lista_de_registros:
            for chave, valor in registro.items():
                if pd.isna(valor):
                    registro[chave] = None
                elif isinstance(valor, pd.Timestamp):
                    registro[chave] = valor.isoformat()
                elif chave in ['sum_orders'] and isinstance(valor, float):
                    registro[chave] = int(valor)
        database.supabase.table('lamonica').upsert(lista_de_registros, on_conflict='trip_number').execute()
        return jsonify({'mensagem': f'{len(lista_de_registros)} registros importados com sucesso!'})
    except Exception as e:
        return jsonify({'erro': f'Ocorreu um erro na importação: {str(e)}'}), 500


@app.route('/api/etas', methods=['GET'])
def get_etas_data():
    response = database.supabase.table('etas').select('*').order('trip_number').execute()
    return jsonify(response.data)


@app.route('/api/etas/<trip_number>', methods=['GET'])
def get_etas_trip(trip_number):
    viagem = database.buscar_etas_por_trip_number(trip_number)
    if viagem: return jsonify(viagem)
    return jsonify({'erro': 'Viagem não encontrada em ETAS'}), 404


@app.route('/api/etas/<trip_number>', methods=['PUT'])
def update_etas_trip(trip_number):
    try:
        dados_recebidos = request.json
        viagem_atualizada = database.atualizar_viagem_etas(trip_number, dados_recebidos)
        if not viagem_atualizada: raise Exception("Falha ao atualizar a viagem no banco de dados.")
        dados_historico = {'trip_number': trip_number, 'operador_nome': 'Operador Padrão', 'acao': 'MODIFICACAO_MANUAL',
                           'detalhes': {'campos_alterados': dados_recebidos}}
        database.registrar_historico(dados_historico)
        return jsonify(viagem_atualizada)
    except Exception as e:
        print(f"ERRO AO ATUALIZAR ETAS: {e}")
        return jsonify({'erro': str(e)}), 500


@app.route('/api/promover-para-etas', methods=['POST'])
def promover_para_etas():
    try:
        dados_viagem = request.json
        trip_number = dados_viagem.get('trip_number')
        if not trip_number: return jsonify({'erro': 'trip_number é obrigatório'}), 400
        dados_viagem['status_agrupado'] = 'ETA ORIGEM'
        dados_viagem['eta_origin_realized'] = datetime.now(timezone.utc).isoformat()
        dados_viagem.pop('id', None);
        dados_viagem.pop('created_at', None)
        viagem_promovida_resp = database.supabase.table('etas').upsert(dados_viagem,
                                                                       on_conflict='trip_number').execute()
        viagem_promovida = viagem_promovida_resp.data[0]
        database.deletar_lamonica_por_trip_number(trip_number)
        dados_historico = {'trip_number': trip_number, 'viagem_id': viagem_promovida['id'],
                           'operador_nome': 'Operador Padrão', 'acao': 'PROMOCAO_PARA_ETAS',
                           'detalhes': {'status_definido': 'ETA ORIGEM'}}
        database.registrar_historico(dados_historico)
        return jsonify({'mensagem': f'Viagem {trip_number} promovida para ETAS com sucesso!'})
    except Exception as e:
        print(f"ERRO DETALHADO AO PROMOVER: {e}")
        return jsonify({'erro': f'Ocorreu um erro ao promover: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(debug=True)