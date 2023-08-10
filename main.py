import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipelineoptions = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipelineoptions)

colunas_dengue=[

        'id',
        'data_iniSE',
        'casos',
        'ibge_code',
        'cidade',
        'uf',
        'cep',
        'latitude',
        'longitude'
]

def lista_para_dicionario(elemento,coluna):
    """
    Recebe Listas,
    Retorna Dicionário
    """
    return dict(zip(coluna,elemento))
    
def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe texto e delimitador
    e 
    Retorna uma lista de elementos
    """
    return elemento.split(delimitador)

def trata_datas(elemento):
    """
    Recebe um dicionário
    Cria novo campo com Ano-Mes
    Retorna Dicionário tratado
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento


def chave_uf(elemento):
    """
    Recebe Dicionário
    Retorna Tupla com a UF e o Elemento (UF,dicionário)
    """
    chave=elemento['uf']
    return (chave,elemento)

def casos_dengue(elemento):
    """
    Recebe tupla crua
    Retorna tupla tratada
    """
    uf, registros = elemento
    for registro in registros:
        if registro['casos'] == '':
            registro['casos'] = '0.0'
        else:
            yield (f"{uf}-{registro['ano_mes']}",float(registro['casos']))

dengue = ( #Pcollection - Dengue
    pipeline
    | "Leitura do dataset de dengue" >> 
    ReadFromText('casos_dengue.txt',skip_header_lines=1)

    | "De texto para lista" >> 
    beam.Map(texto_para_lista)
    
    | "De lista para dicionário" >>
    beam.Map(lista_para_dicionario,colunas_dengue)

    |"Criar campo ano_mes" >>
    beam.Map(trata_datas)

    |"Agrupar por estado (1)" >>
    beam.Map(chave_uf)

    |"Agrupar por estado (2)" >> 
    beam.GroupByKey()

    |"Descompactar casos de dengue" >>
    beam.FlatMap(casos_dengue)

    |"Soma dos casos por chave" >>
    beam.CombinePerKey(sum)

    # | "Mostrar" >>
    # beam.Map(print)


)

def chave_uf_ano_mes_de_lista(elemento):
    """
    Recebe lista de elementos
    Retorna Tupla com chave e o valor de chuva em mm
    ('UF-ANO-MES',1.3)
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm=0.0
    return(chave,float(mm))

def arredonda(elemento):
    """
    Recebe tupla 
    Retorna tupla arredondada
    """
    chave,mm = elemento
    return (chave,round(mm,1))

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> 
    ReadFromText('chuvas.csv',skip_header_lines=1)

    | "Texto para listas (2)" >>
    beam.Map(texto_para_lista,delimitador=',') 

    | "Cria chave uf-ano-mes" >>
    beam.Map(chave_uf_ano_mes_de_lista)

    | "Soma dos mm por mes e ano" >>
    beam.CombinePerKey(sum)

    | "Arredondar resultados" >>
    beam.Map(arredonda)

    # | "Mostrar (2)" >>
    # beam.Map(print)   


)


def filtra_campos_vazios(elemento):
    """
    Remove elementos com chaves vazias
    """
    chave,dados = elemento
    if all([
        dados['chuvas'],
        dados['dengue']
        ]):
        return True
    return False

def descompactar_elementos(elemento):
    """
    Recebe tupla,
    Retorna tupla descompactada
    """
    chave, dados = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(dengue)

def preparar_csv(elem,delimitador=';'):
    """
    Recebe tupla,
    retorna string separada por ';'
    """   
    return f"{delimitador}".join(elem)

resultado=(
    ({'chuvas':chuvas,'dengue':dengue})

    | "Mesclar Pcollections" >>
    beam.CoGroupByKey() 

    | "Filtrar dados vazios" >>
    beam.Filter(filtra_campos_vazios)

    | "Descompactar dicionários" >>
    beam.Map(descompactar_elementos)

    | "Formatar dados" >> 
    beam.Map(preparar_csv)

    # | "Mostrar resultado" >>
    # beam.Map(print)

)

header = 'UF;ANO;MES;CHUVA(MM);DENGUE(CASOS)'
resultado | "Criar arquivo CSV" >> WriteToText('resultado',file_name_suffix= '.csv',header=header)

pipeline.run()
