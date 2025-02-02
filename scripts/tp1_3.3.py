import psycopg2
import os
from datetime import datetime
import prettytable
today = datetime.now()
data_string=today.strftime("_%d%m%Y_%H%M%S.txt")

arquivo = input("Realizar consultas manualmente? [y][n]").lower()
nome_arq_saida = "saida/saida_tp1_3.3"
nome_arq_saida += data_string
arq_saida = open(nome_arq_saida,"x")

# Parâmetros de conexão com o banco de dados
connection = {
    'database': 'postgres',
    'user': 'postgres',
    'host': 'localhost',
    'password': '123',
    'port': 5432,
}
a = [
    "SELECT * FROM reviews WHERE asin = '{asin}' AND helpful IS NOT NULL AND rating IS NOT NULL ORDER BY rating DESC, helpful DESC LIMIT 5;",
    "SELECT * FROM reviews WHERE asin = '{asin}' AND helpful IS NOT NULL AND rating IS NOT NULL ORDER BY rating ASC, helpful DESC LIMIT 5;"
]
b = ["SELECT asin_similar AS similar,product_title as title, product_salesrank AS salesrank FROM similars,products WHERE similars.asin = '{asin}' AND products.asin = asin_similar AND product_salesrank < (SELECT product_salesrank FROM products WHERE asin = '{asin}') AND product_salesrank > 0"]
c = ["SELECT asin ,review_date AS data, AVG(rating) OVER (PARTITION BY asin ORDER BY review_date) AS media_avaliacao FROM reviews WHERE asin = '{asin}';"]
d = ["SELECT product_id,product_title, product_group, product_salesrank FROM (SELECT product_id, product_group, product_salesrank,product_title, ROW_NUMBER() OVER (PARTITION BY product_group ORDER BY product_salesrank) AS rank FROM products WHERE product_salesrank IS NOT NULL AND product_salesrank > 0) ranked WHERE rank <= 10;"]
e = ["SELECT products.product_id ,products.product_title, AVG(reviews.helpful) AS Media_Avaliacoes_Uteis FROM reviews JOIN products ON reviews.asin = products.asin WHERE reviews.rating > 0 GROUP BY products.product_id ORDER BY AVG(reviews.helpful) DESC LIMIT 10;"]
f = ["SELECT product_categories.category_name AS Category_Name, AVG(reviews.helpful) AS Media_Avaliacoes_Uteis FROM reviews JOIN product_categories ON reviews.asin = product_categories.asin WHERE reviews.rating > 0 GROUP BY product_categories.category_name ORDER BY AVG(reviews.helpful) DESC LIMIT 5;"]
g = ["SELECT client_id, product_group, Total_Comentarios FROM (SELECT r.*, ROW_NUMBER() OVER (PARTITION BY product_group ORDER BY product_group) AS rank FROM (SELECT reviews.id_customer AS client_id, products.product_group, COUNT(reviews.id_customer) AS Total_Comentarios FROM reviews JOIN products ON reviews.asin = products.asin GROUP BY products.product_group, reviews.id_customer ORDER BY products.product_group, Total_Comentarios DESC) AS r) as r WHERE rank < 11;"]
consultas = {'a': a, 'b': b, 'c': c, 'd': d, 'e': e, 'f': f, 'g': g}

ha=["review_date", "id_customer", "rating", "votes", "helpful", "asin", "review_id"]
hb=["similar", "title", "salesrank"] 
hc=["asin","data","media_avaliacao"]
hd=["product_id","product_title","product_group","product_salesrank"]
he=["product_id","product_title","Media_Avaliacoes_Uteis"]
hf=["Category_Name","Media_Avaliacoes_Uteis"]
hg=["client_id", "product_group", "Total_Comentarios"]

headersTabelas = {'a':ha, 'b':hb, 'c':hc, 'd':hd, 'e':he, 'f':hf, 'g':hg }

def func_principal(consultaEscolhida):
    for query in consultas[consultaEscolhida]:
        with psycopg2.connect(**connection) as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(query)
                    resultados = cursor.fetchall()
                    table = prettytable.PrettyTable()
                    table.field_names = headersTabelas[consultaEscolhida]
                    for result in resultados:
                        table.add_row(result)
                    arq_saida.write(f"{table}\n\n")
                        
                except Exception as e:
                    print(f'Ocorreu um erro: {e}')
    

if(arquivo == 'y'):
    while True:
        consultaEscolhida = input('Escolha entre as opcoes de consulta:').lower()
        if consultaEscolhida in consultas.keys():
            if(consultaEscolhida == 'a' or consultaEscolhida == 'b' or consultaEscolhida == 'c'):
                userInput = input('Digite o produto: ')
                consultas[consultaEscolhida] = [query.format(asin=userInput) for query in consultas[consultaEscolhida]]
                arq_saida.write(f"Iniciando consulta: {consultaEscolhida} e asin: {userInput}\n")
                func_principal(consultaEscolhida)
            else:
                arq_saida.write(f"Iniciando consulta: {consultaEscolhida}\n")
                func_principal(consultaEscolhida)
        else:
            print("Escolha uma opcao correta do dashboard")
        
    
elif(arquivo == 'n'):
    asin = "1559362022"
    arq_saida.write(f"\niniciando consultas considerando asin = \"{asin}\"\n")
    for consulta in consultas:
        print(f"\nIniciando consulta: {consulta}\n")
        arq_saida.write(f"\nIniciando consulta: {consulta}\n")
        consultas[consulta] = [query.format(asin=asin) for query in consultas[consulta]]
        func_principal(consulta)
    arq_saida.close()
    
