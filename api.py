from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import re
from datetime import datetime, timedelta
from collections import Counter

app = Flask(__name__)
CORS(app)

# ===================== SUAS FUNÇÕES =====================

def brl_para_float(celula):
    if pd.isna(celula):
        return 0.0
    texto = str(celula).strip()
    if isinstance(celula, (int, float)):
        return float(celula)
    texto_limpo = re.sub(r'[^\d.,]', '', texto)
    if not texto_limpo:
        return 0.0
    if ',' in texto_limpo:
        sem_pontos = texto_limpo.replace('.', '')
        com_ponto = sem_pontos.replace(',', '.')
        try:
            return float(com_ponto)
        except:
            return 0.0
    else:
        try:
            return float(texto_limpo)
        except:
            return 0.0

def formatar_brl(val):
    if val == 0:
        return "R$ 0,00"
    return f"R$ {val:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def padronizar_codigo(codigo):
    if pd.isna(codigo):
        return ""
    try:
        partes = str(codigo).strip().split('.')
        return '.'.join(str(int(p)) for p in partes if p.strip() != '')
    except:
        return str(codigo).strip()

def get_nivel(codigo):
    if pd.isna(codigo):
        return 'Não Classificado'
    count = str(codigo).count('.')
    return {0: 'Fase', 1: 'Etapa', 2: 'Atividade', 3: 'SubAtividade', 4: 'SubSubAtividade'}.get(count, 'Não Classificado')

def parse_date(text):
    if pd.isna(text) or str(text).strip() in ['', 'NA', 'None', 'nan', '#N/D', '#n/d']:
        return None
    text = str(text).strip()
    correcoes = {'Wen': 'Wed', 'Qui': 'Thu', 'Qua': 'Wed', 'Sex': 'Fri', 'Seg': 'Mon', 'Ter': 'Tue', 'Sab': 'Sat', 'Dom': 'Sun'}
    for k, v in correcoes.items():
        if text.startswith(k):
            text = v + text[len(k):]
    for fmt in ['%a %d/%m/%y', '%a %d/%m/%Y', '%d/%m/%y', '%d/%m/%Y', '%Y-%m-%d']:
        try:
            return datetime.strptime(text, fmt).date()
        except:
            continue
    return None

def dias_uteis_entre(inicio, fim):
    if not inicio or not fim or inicio > fim:
        return 0
    return sum(1 for i in range((fim - inicio).days + 1)
               if (inicio + timedelta(days=i)).weekday() < 5)

def parse_percentual(valor):
    if pd.isna(valor):
        return 0
    try:
        if isinstance(valor, (int, float)):
            if 0 <= valor <= 1:
                return round(valor * 100, 1)
            elif 0 <= valor <= 100:
                return round(valor, 1)
            else:
                return 0
        valor_str = str(valor).strip().replace(' ', '').replace('%', '')
        if not valor_str or valor_str.lower() in ['nan', 'na', 'n/a', '']:
            return 0
        valor_str = valor_str.replace(',', '.')
        valor_num = float(valor_str)
        if valor_num > 1:
            return round(min(valor_num, 100), 1)
        else:
            return round(valor_num * 100, 1)
    except:
        return 0

def classificar_andamento(row, hoje):
    if row.get('termino_real'):
        return "Concluídas"
    if row.get('inicio_real'):
        return "Em execução"
    if row.get('inicio_planejado') and row['inicio_planejado'] < hoje:
        return "Atrasadas"
    if row.get('inicio_planejado') and hoje <= row['inicio_planejado'] <= hoje + timedelta(days=15):
        return "Previstas (15 dias)"
    if row.get('inicio_planejado'):
        return "Planejadas"
    return "Sem classificação"

# ===================== ENDPOINT PRINCIPAL =====================

@app.route('/processar', methods=['POST'])
def processar():
    try:
        # Recebe dados do n8n
        data = request.get_json()
        
        # Extrai os itens
        items = data.get('items', [])
        
        if not items:
            return jsonify({'error': 'Nenhum dado recebido'}), 400
        
        # Converte para DataFrame
        dados = [item.get('json', item) for item in items]
        df = pd.DataFrame(dados)
        
        hoje_completo = datetime.now() - timedelta(hours=3)
        hoje = hoje_completo.date()
        
        # Processa os dados
        df['codigo_limpo'] = df['Item'].apply(padronizar_codigo)
        df['Nível'] = df['Item'].apply(get_nivel)
        df['inicio_planejado'] = df['Início (Planejado LB)'].apply(parse_date)
        df['termino_planejado'] = df['Término (Planejado LB)'].apply(parse_date)
        df['inicio_real'] = df['Início (Real)'].apply(parse_date)
        df['termino_real'] = df['Término (Real)'].apply(parse_date)
        df['Status Andamento'] = df.apply(lambda row: classificar_andamento(row, hoje), axis=1)
        
        # Calcula totais financeiros
        total_exec = brl_para_float(df.iloc[0].get('R$ Total Execução', 0))
        planejamento = brl_para_float(df.iloc[0].get('R$ Planejamento', 0))
        executado = brl_para_float(df.iloc[0].get('R$ Real Executado', 0))
        restante = brl_para_float(df.iloc[0].get('R$ Restante de Execução', 0))
        
        totais_financeiros = {
            "total_execucao": formatar_brl(total_exec),
            "r_planejamento": formatar_brl(planejamento),
            "r_real_executado": formatar_brl(executado),
            "r_restante": formatar_brl(restante)
        }
        
        # Prepara resultados simplificados
        resultados = []
        for _, row in df.iterrows():
            ip = row['inicio_planejado']
            tp = row['termino_planejado']
            ir = row['inicio_real']
            tr = row['termino_real']
            
            # Calcular % ideal
            ideal = 0
            if ip and tp:
                if tr:
                    ideal = 100
                else:
                    base = ir if ir else ip
                    total_dias = dias_uteis_entre(base, tp)
                    if total_dias > 0:
                        decorridos = min(dias_uteis_entre(base, hoje), total_dias)
                        ideal = round((decorridos / total_dias) * 100, 1)
            
            resultados.append({
                "codigo": str(row.get('Item', '')),
                "atividade": str(row.get('Atividades de Planejamento', '')).strip(),
                "nivel": row['Nível'],
                "status_andamento": row['Status Andamento'],
                "inicio_planejado": ip.strftime('%d/%m/%Y') if ip else None,
                "termino_planejado": tp.strftime('%d/%m/%Y') if tp else None,
                "inicio_real": ir.strftime('%d/%m/%Y') if ir else None,
                "termino_real": tr.strftime('%d/%m/%Y') if tr else None,
                "pct_planejada": 100,
                "pct_concluida": parse_percentual(row.get('% Concluída', 0)),
                "pct_ideal_ate_hoje": ideal,
                "r_planejamento": formatar_brl(brl_para_float(row.get('R$ Planejamento', 0))),
                "r_real_executado": formatar_brl(brl_para_float(row.get('R$ Real Executado', 0)))
            })
        
        # Retorna no formato que o n8n espera
        return jsonify([{
            'json': {
                'data_hoje': hoje_completo.strftime('%d/%m/%Y às %H:%M'),
                'total_atividades': len(resultados),
                'todas_as_atividades': resultados,
                'totais_financeiros': totais_financeiros,
                'resumo_andamento': df['Status Andamento'].value_counts().to_dict()
            }
        }])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/', methods=['GET'])
@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'message': 'API do processador n8n está funcionando!'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
