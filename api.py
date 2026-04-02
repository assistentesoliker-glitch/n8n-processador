from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import re
from datetime import datetime, timedelta
from collections import Counter

app = Flask(__name__)
CORS(app)

# ===================== FUNÇÃO 1: CONVERSÃO DE MOEDA =====================
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

def formatar_moeda(valor):
    if pd.isna(valor):
        return "R$ 0,00"
    if isinstance(valor, str) and valor.startswith('R$'):
        return valor
    try:
        valor_float = brl_para_float(valor)
        return formatar_brl(valor_float)
    except:
        return "R$ 0,00"

# ===================== FUNÇÃO 2: PADRONIZAR CÓDIGOS =====================
def padronizar_codigo(codigo):
    if pd.isna(codigo):
        return ""
    try:
        partes = str(codigo).strip().split('.')
        return '.'.join(str(int(p)) for p in partes if p.strip() != '')
    except:
        return str(codigo).strip()

# ===================== FUNÇÃO 3: NÍVEL DA ATIVIDADE =====================
def get_nivel(codigo):
    if pd.isna(codigo):
        return 'Não Classificado'
    count = str(codigo).count('.')
    return {0: 'Fase', 1: 'Etapa', 2: 'Atividade', 3: 'SubAtividade', 4: 'SubSubAtividade'}.get(count, 'Não Classificado')

# ===================== FUNÇÃO 4: PARSE DE DATA =====================
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

# ===================== FUNÇÃO 5: DIAS ÚTEIS =====================
def dias_uteis_entre(inicio, fim):
    if not inicio or not fim or inicio > fim:
        return 0
    return sum(1 for i in range((fim - inicio).days + 1)
               if (inicio + timedelta(days=i)).weekday() < 5)

# ===================== FUNÇÃO 6: CONVERTER PORCENTAGEM =====================
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

# ===================== FUNÇÃO 7: STATUS DE ANDAMENTO =====================
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

# ===================== FUNÇÃO 8: PREENCHER SETORES VAZIOS =====================
def preencher_setores_vazios(df):
    df['Setor_preenchido'] = df['Setor'].copy()
    for idx, row in df.iterrows():
        nivel = row['Nível']
        if nivel == 'Atividade':
            codigo_atividade = row['codigo_limpo']
            setor_atividade = str(row['Setor']).strip()
            if not setor_atividade or setor_atividade.lower() in ['', 'nan', 'none', 'n/a']:
                codigo_base = codigo_atividade + '.'
                subatividades = df[df['codigo_limpo'].str.startswith(codigo_base, na=False)]
                setores_subatividades = []
                for _, sub_row in subatividades.iterrows():
                    setor_sub = str(sub_row['Setor']).strip()
                    if setor_sub and setor_sub.lower() not in ['', 'nan', 'none', 'n/a']:
                        setores_subatividades.append(setor_sub)
                if setores_subatividades:
                    contador = Counter(setores_subatividades)
                    setor_mais_comum = contador.most_common(1)[0][0]
                    df.at[idx, 'Setor_preenchido'] = setor_mais_comum
                    df.at[idx, 'Setor'] = setor_mais_comum
    return df

# ===================== FUNÇÃO 9: LIMPAR DEPENDÊNCIAS =====================
def limpar_wbs(campo):
    if pd.isna(campo) or not str(campo).strip():
        return []
    texto = re.sub(r'\[.*?\]', '', str(campo))
    itens = [x.strip() for x in re.split(r'[;,]\s*', texto) if x.strip()]
    return [padronizar_codigo(i) for i in itens if i and i.lower() not in ['none', '']]

# ===================== FUNÇÃO 10: EXTRAIR QTD DIÁRIA =====================
def extrair_qtd_diaria(valor):
    if pd.isna(valor):
        return 0
    try:
        if isinstance(valor, (int, float)):
            return float(valor)
        valor_str = str(valor).strip()
        if valor_str == "#ERROR" or valor_str.lower() == "error":
            return 0
        valor_limpo = re.sub(r'[^\d.,]', '', valor_str)
        if not valor_limpo:
            return 0
        if ',' in valor_limpo:
            sem_pontos = valor_limpo.replace('.', '')
            com_ponto = sem_pontos.replace(',', '.')
            return float(com_ponto)
        else:
            return float(valor_limpo)
    except:
        return 0

# ===================== FUNÇÃO 11: TRATAR LOCAÇÃO DE RECURSOS =====================
def tratar_locacao_recursos(locacao_raw):
    if pd.isna(locacao_raw):
        return '-'
    locacao_str = str(locacao_raw).strip()
    if not locacao_str or locacao_str.lower() in ['', 'nan', 'none', 'n/a']:
        return '-'
    padrao_valor_venda = r'^\d+(\.\d+)*\s*-\s*Valor\s*Venda\s*\[\d+\]\s*$'
    if re.match(padrao_valor_venda, locacao_str, re.IGNORECASE):
        return '-'
    padrao_remover = r'^\d+(\.\d+)*\s*-\s*Valor\s*Venda\s*\[\d+\]\s*;\s*'
    locacao_limpa = re.sub(padrao_remover, '', locacao_str, flags=re.IGNORECASE)
    if not locacao_limpa or locacao_limpa.strip() == '':
        return '-'
    return locacao_limpa.strip()

# ===================== FUNÇÃO 12: TRATAR MARGEM DE ATRASO =====================
def processar_margem_atraso(margem_raw):
    if pd.isna(margem_raw):
        return {'texto_formatado': '-', 'dias': 0, 'is_zero_days': False}
    margem_str = str(margem_raw).strip()
    if not margem_str or margem_str.lower() in ['', 'nan', 'none', 'n/a']:
        return {'texto_formatado': '-', 'dias': 0, 'is_zero_days': False}
    margem_traduzida = margem_str.replace('days', 'dias').replace('day', 'dia')
    padrao_zero_exato = r'^\s*0\s*(?:days|dias)\s*$'
    padrao_numero = r'(\d+)\s*(?:days|dias)'
    is_zero_days = False
    dias = 0
    if re.match(padrao_zero_exato, margem_str, re.IGNORECASE):
        is_zero_days = True
        dias = 0
    else:
        try:
            match = re.search(padrao_numero, margem_str, re.IGNORECASE)
            if match:
                dias = int(match.group(1))
                is_zero_days = (dias == 0)
            else:
                try:
                    dias_num = float(margem_str)
                    dias = int(dias_num)
                    is_zero_days = (dias == 0)
                except:
                    dias = 0
                    is_zero_days = False
        except:
            dias = 0
            is_zero_days = False
    return {'texto_formatado': margem_traduzida, 'dias': dias, 'is_zero_days': is_zero_days}

# ===================== FUNÇÃO 13: TRADUZIR VARIAÇÕES =====================
def traduzir_variacao(var_str):
    if pd.isna(var_str) or not str(var_str).strip():
        return '-'
    var_str_clean = str(var_str).strip()
    if var_str_clean == '' or var_str_clean == '0':
        return var_str_clean
    var_traduzida = var_str_clean
    var_traduzida = re.sub(r'(\d+)\s*days', r'\1 dias', var_traduzida, flags=re.IGNORECASE)
    var_traduzida = re.sub(r'(\d+)\s*day', r'\1 dia', var_traduzida, flags=re.IGNORECASE)
    return var_traduzida

# ===================== ENDPOINT PRINCIPAL =====================

@app.route('/processar', methods=['POST'])
def processar():
    try:
        data = request.get_json()
        items = data.get('items', [])
        
        if not items:
            return jsonify({'error': 'Nenhum dado recebido'}), 400
        
        dados = [item.get('json', item) for item in items]
        df = pd.DataFrame(dados)
        
        hoje_completo = datetime.now() - timedelta(hours=3)
        hoje = hoje_completo.date()
        
        # ===================== PROCESSAMENTO =====================
        
        # Totais financeiros da linha 0
        linha_resumo = df.iloc[0]
        total_exec = brl_para_float(linha_resumo.get('R$ Total Execução', 0))
        planejamento = brl_para_float(linha_resumo.get('R$ Planejamento', 0))
        executado = brl_para_float(linha_resumo.get('R$ Real Executado', 0))
        restante = brl_para_float(linha_resumo.get('R$ Restante de Execução', 0))
        
        totais_financeiros = {
            "total_execucao": formatar_brl(total_exec),
            "r_planejamento": formatar_brl(planejamento),
            "r_real_executado": formatar_brl(executado),
            "r_restante": formatar_brl(restante),
            "fonte_calculo": "Linha 2 da planilha (resumo oficial)"
        }
        
        # Padronização de códigos
        df['codigo_limpo'] = df['Item'].apply(padronizar_codigo)
        df['Nível'] = df['Item'].apply(get_nivel)
        
        # Parse de datas
        df['inicio_planejado'] = df['Início (Planejado LB)'].apply(parse_date)
        df['termino_planejado'] = df['Término (Planejado LB)'].apply(parse_date)
        df['inicio_real'] = df['Início (Real)'].apply(parse_date)
        df['termino_real'] = df['Término (Real)'].apply(parse_date)
        
        # Preencher setores
        df = preencher_setores_vazios(df)
        
        # Status de andamento
        df['Status Andamento'] = df.apply(lambda row: classificar_andamento(row, hoje), axis=1)
        
        # Limpeza de dependências
        df['pred_limpos'] = df.get('WBS Predecessors', '').apply(limpar_wbs)
        df['suc_limpos'] = df.get('SuccessorsWBS', '').apply(limpar_wbs)
        
        # Cálculo de valores
        valores_planejados = {}
        valores_executados = {}
        for _, row in df.iterrows():
            codigo = row['codigo_limpo']
            if codigo:
                valores_planejados[codigo] = brl_para_float(row.get('R$ Planejamento', 0))
                valores_executados[codigo] = brl_para_float(row.get('R$ Real Executado', 0))
        
        # Preparar resultados
        resultados = []
        for _, row in df.iterrows():
            ip = row['inicio_planejado']
            tp = row['termino_planejado']
            ir = row['inicio_real']
            tr = row['termino_real']
            real = parse_percentual(row.get('% Concluída', 0))
            planejado = parse_percentual(row.get('% Planejada', 100))
            if planejado == 0:
                planejado = 100
            
            ideal = 0
            if ip and tp:
                if tr:
                    ideal = 100
                else:
                    base = ir if ir else ip
                    total_dias = dias_uteis_entre(base, tp)
                    if total_dias > 0:
                        decorridos = min(dias_uteis_entre(base, hoje), total_dias)
                        ideal = round((decorridos / total_dias) * planejado, 1)
            
            alerta = "Planejada"
            if tr:
                alerta = "Concluída"
            elif tp and tp < hoje and real < 100:
                alerta = "Atrasada – Acionar Plano de Recuperação"
            elif row['Status Andamento'] == "Atrasadas":
                alerta = "Atrasada – Monitorar"
            elif row['Status Andamento'] == "Em execução":
                diff = real - ideal
                if diff > 5:
                    alerta = "Adiantada"
                elif diff < -5:
                    alerta = "Em atraso de produtividade"
                else:
                    alerta = "No ritmo"
            
            # Margem de atraso
            margem_raw = str(row.get('Margem de Atraso', '')).strip()
            margem_info = processar_margem_atraso(margem_raw)
            
            # Variações
            start_variance = traduzir_variacao(row.get('Start Variance', ''))
            finish_variance = traduzir_variacao(row.get('Finish Variance', ''))
            duration_variance = traduzir_variacao(row.get('Duration Variance', ''))
            
            # Locação de recursos
            locacao_recursos = tratar_locacao_recursos(row.get('Locação Recurso', ''))
            
            # Setor
            setor_atividade = str(row.get('Setor_preenchido', row.get('Setor', ''))).strip()
            if setor_atividade.lower() in ['', 'nan', 'none', 'n/a']:
                setor_atividade = ''
            
            # Quantidade diária
            qtd_diaria = extrair_qtd_diaria(row.get('Quantidade Planejada Diária', 0))
            
            # Duração planejada
            duracao_planejada_raw = str(row.get('Duração (Planejado LB)', '')).strip()
            if duracao_planejada_raw and duracao_planejada_raw.lower() not in ['', 'nan', 'none', 'n/a']:
                duracao_limpa = duracao_planejada_raw.replace('days', '').replace('dias', '').strip()
                if duracao_limpa:
                    try:
                        valor_num = float(duracao_limpa.split()[0]) if ' ' in duracao_limpa else float(duracao_limpa)
                        duracao_planejada = f"{valor_num} dia" if valor_num == 1 else f"{valor_num} dias"
                    except:
                        duracao_planejada = duracao_limpa
                else:
                    duracao_planejada = "-"
            else:
                duracao_planejada = "-"
            
            # Valores financeiros
            valor_total_exec = formatar_moeda(row.get('R$ Total Execução', ''))
            valor_planejado = formatar_moeda(row.get('R$ Planejamento', ''))
            valor_executado = formatar_moeda(row.get('R$ Real Executado', ''))
            valor_planejado_num = brl_para_float(row.get('R$ Planejamento', 0))
            valor_executado_num = brl_para_float(row.get('R$ Real Executado', 0))
            saldo_atividade = valor_executado_num - valor_planejado_num
            
            resultados.append({
                "codigo": str(row.get('Item', '')),
                "atividade": str(row.get('Atividades de Planejamento', 'Sem descrição')).strip(),
                "nivel": row['Nível'],
                "status_andamento": row['Status Andamento'],
                "status_atividades": str(row.get('Status Atividades', '')).strip(),
                "inicio_planejado": ip.strftime('%d/%m/%Y') if ip else None,
                "termino_planejado": tp.strftime('%d/%m/%Y') if tp else None,
                "inicio_real": ir.strftime('%d/%m/%Y') if ir else None,
                "termino_real": tr.strftime('%d/%m/%Y') if tr else None,
                "start_variance": start_variance,
                "finish_variance": finish_variance,
                "duration_variance": duration_variance,
                "margem_atraso": margem_info['texto_formatado'],
                "dias_margem_atraso": margem_info['dias'],
                "is_critica_margem_zero": margem_info['is_zero_days'],
                "peso_financeiro_pct": str(row.get('% Peso Financeiro', '')),
                "r_planejamento": valor_planejado,
                "r_real_executado": valor_executado,
                "r_restante": formatar_moeda(row.get('R$ Restante de Execução', '')),
                "r_total_execucao": valor_total_exec,
                "pct_planejada": planejado,
                "pct_concluida": real,
                "pct_ideal_ate_hoje": ideal,
                "alerta_produtividade": alerta,
                "predecessores": [],
                "sucessores": [],
                "is_milestone": str(row.get('Milestone', 'no')).strip().lower() in ['yes', 'sim', 's', 'y', 'true', '1', 'verdadeiro', 'ok'],
                "setor": setor_atividade,
                "locacao_recurso": locacao_recursos,
                "unidade_trabalho": str(row.get('Unidade de Trabalho', '')),
                "quantidade_total": str(row.get('Quantidade Total de Medição', '')),
                "quantidade_planejada_diaria": qtd_diaria,
                "duracao_planejada": duracao_planejada,
                "observacoes": str(row.get('Observações - Gestão Medição', '') or ''),
                "saldo_atividade": formatar_brl(saldo_atividade),
                "empreendimento": str(row.get('Empreendimento', '')),
                "calendario_projeto": str(row.get('Calendário de Projeto', '')),
                "predecessors": str(row.get('Predecessors', '')),
                "successors": str(row.get('Successors', ''))
            })
        
        # Filtro hierárquico
        def deve_mostrar_na_tab(atividade):
            codigo = atividade.get('codigo', '')
            nivel = atividade.get('nivel', '')
            try:
                fase = int(codigo.split('.')[0])
            except:
                return nivel in ['Atividade', 'SubAtividade']
            if fase == 10:
                return False
            if fase == 2:
                return nivel == 'Atividade'
            if fase in [4, 5]:
                return nivel == 'SubAtividade'
            if fase in [1, 3, 6, 7, 8, 9]:
                return nivel == 'Atividade'
            return nivel in ['Atividade', 'SubAtividade']
        
        resultados_filtrados = [atv for atv in resultados if deve_mostrar_na_tab(atv)]
        
        # Atividades críticas
        atv_IA = []
        for atividade in resultados_filtrados:
            nivel = atividade.get('nivel', '')
            status = atividade.get('status_andamento', '')
            if nivel == 'Atividade':
                if status == 'Atrasadas':
                    atv_IA.append(dict(atividade))
                elif status == 'Em execução':
                    pct_planejada = atividade.get('pct_planejada', 100)
                    pct_real = atividade.get('pct_concluida', 0)
                    if pct_real < pct_planejada:
                        atv_IA.append(dict(atividade))
        
        # Estatísticas
        status_counts = {
            "Total": len(resultados_filtrados),
            "Concluídas": len([r for r in resultados_filtrados if r['status_andamento'] == 'Concluídas']),
            "Atrasadas": len([r for r in resultados_filtrados if r['status_andamento'] == 'Atrasadas']),
            "Em execução": len([r for r in resultados_filtrados if r['status_andamento'] == 'Em execução']),
            "Previstas (15 dias)": len([r for r in resultados_filtrados if r['status_andamento'] == 'Previstas (15 dias)']),
            "Planejadas": len([r for r in resultados_filtrados if r['status_andamento'] == 'Planejadas'])
        }
        
        # ===================== SAÍDA =====================
        return jsonify([{
            'json': {
                'data_hoje': hoje_completo.strftime('%d/%m/%Y às %H:%M'),
                'total_atividades': len(resultados_filtrados),
                'atividades_para_cards': len([r for r in resultados_filtrados if r['nivel'] in ['Atividade', 'SubAtividade']]),
                'valor_total_projeto': formatar_brl(sum(valores_planejados.values())),
                'resumo_niveis': df['Nível'].value_counts().to_dict(),
                'resumo_andamento': df['Status Andamento'].value_counts().to_dict(),
                'totais_financeiros': totais_financeiros,
                'todas_as_atividades': resultados_filtrados,
                'todas_as_atividades_completas': resultados,
                'atv_n3': [r for r in resultados_filtrados if r['nivel'] in ['Atividade', 'SubAtividade']],
                'atv_IA': atv_IA,
                'estatisticas_atv_IA': {
                    'total_criticas': len(atv_IA),
                    'atrasadas': len([a for a in atv_IA if a['status_andamento'] == 'Atrasadas']),
                    'em_execucao_produtividade_atrasada': len([a for a in atv_IA if a['status_andamento'] == 'Em execução'])
                },
                'estatisticas_cards': status_counts,
                'grafico_fisico': {
                    "Concluídas": status_counts["Concluídas"],
                    "Atrasadas": status_counts["Atrasadas"],
                    "Em execução": status_counts["Em execução"],
                    "Previstas (15 dias)": status_counts["Previstas (15 dias)"],
                    "Planejadas": status_counts["Planejadas"]
                },
                'metricas_impacto': {
                    'valor_total_projeto': formatar_brl(sum(valores_planejados.values())),
                    'contagem_por_criticidade': {'Baixa': 0, 'Média': 0, 'Alta': 0, 'Crítica': 0}
                },
                'resumo_atividades_problema': {
                    'total_analisadas': len([r for r in resultados_filtrados if r['nivel'] == 'Atividade']),
                    'atrasadas': len([r for r in resultados_filtrados if r['nivel'] == 'Atividade' and r['status_andamento'] == 'Atrasadas']),
                    'em_atraso_produtividade': 0,
                    'criticas_margem_zero': len([r for r in resultados_filtrados if r.get('is_critica_margem_zero', False)]),
                    'com_saldo_negativo': len([r for r in resultados_filtrados if r['saldo_atividade'].startswith('-')])
                }
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
