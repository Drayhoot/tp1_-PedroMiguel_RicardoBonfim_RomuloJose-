import sys
import re
import psycopg2

conn = psycopg2.connect(database = "postgres", 
                        user = "postgres", 
                        host= 'localhost',
                        password = "postgres",
                        port = 5432)

def inicializeDB():
    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS products(
                    product_id SERIAL UNIQUE NOT NULL PRIMARY KEY,
                    asin CHAR (10) UNIQUE NOT NULL,
                    product_title VARCHAR (452),
                    product_group VARCHAR (13),
                    product_salesrank INTEGER
                    );
                    """)

        cur.execute("""CREATE TABLE IF NOT EXISTS similars(
                    asin CHAR (10) NOT NULL REFERENCES products(asin),
                    asin_similar CHAR (10) NOT NULL 
                    );
                    """)
                    #asin_similar n eh tratado como chave estrangeira pois existe alguns produtos não instanciados que estavam na área de asin_similars.

        cur.execute("""CREATE TABLE IF NOT EXISTS reviews(
                    review_date DATE NOT NULL,
                    id_customer VARCHAR (18) NOT NULL,
                    rating INTEGER NOT NULL,
                    votes INTEGER NOT NULL,
                    helpful INTEGER NOT NULL,
                    asin CHAR (10) NOT NULL REFERENCES products(asin),
                    review_id SERIAL UNIQUE NOT NULL PRIMARY KEY
                    );
                    """)

        cur.execute("""CREATE TABLE IF NOT EXISTS product_categories(
                    asin CHAR (10) NOT NULL REFERENCES products(asin),
                    category_name VARCHAR (2500) NOT NULL
                    );
                    """)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Não foi possível criar tabelas\n{e}")

def insereProduto(pId='null',asin='null',pTitle='null',pGroup='null',pSalesrank='null'):
    if(pId == 'null' or asin == 'null'):
        return
    if("'" in pTitle):
        pTitle = pTitle.replace("'","''")
    try:
        cur = conn.cursor()
        sql_command = "INSERT INTO products(product_id,asin,product_title,product_group,product_salesrank) VALUES ({}, '{}', '{}', '{}', {})"
        cur.execute(sql_command.format(pId, asin, pTitle, pGroup, pSalesrank))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Erro ao inserir na tabela products\n{e}")

def insereSimilares(asin='null',asin_similar='null'):
    if(asin_similar == 'null' or asin == 'null'):
        return
    try:
        cur = conn.cursor()
        sql_command = "INSERT INTO similars(asin, asin_similar) VALUES ('{}', '{}')"
        cur.execute(sql_command.format(asin, asin_similar))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Erro ao inserir na tabela similars\n{e}")

def insereReviews(review_date='null', id_customer='null', rating='null', votes='null', helpful='null', asin='null'):
    if(id_customer=='null'or rating=='null' or asin=='null'):
        return
    try:
        cur = conn.cursor()
        sql_command = "INSERT INTO reviews(review_date, id_customer, rating, votes, helpful, asin) VALUES ('{}', '{}', {}, {}, {}, '{}')"
        cur.execute(sql_command.format(review_date, id_customer, rating, votes, helpful, asin))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Erro ao inserir na tabela reviews\n{e}")

def insereCategorias(asin='null', category_name='null'):
    if(asin == 'null' or category_name == 'null'):
        return
    if("'" in category_name):
        category_name = category_name.replace("'","''")
    try:
        cur = conn.cursor()
        sql_command = "INSERT INTO product_categories(asin, category_name) VALUES ('{}', '{}')"
        cur.execute(sql_command.format(asin, category_name))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Erro ao inserir na tabela product_categories\n{e}")

def nullOrNot(value):
    if(value != ''):
        return value
    return 'null'

def leProduto(linhas, iAtual, iMax):
    infoProduto = {}
    i = iAtual
    elementoAtual = 0

    # Padroes para a tabela principal
    idPattern = re.compile(r'Id:\s*(\d+)\n')
    asinPattern = re.compile(r'ASIN:\s*([\dA-Z]+)\n')
    titlePattern = re.compile(r'title:\s*(.+)\n')
    groupPattern = re.compile(r'(?<=group:)\s*.*')
    salesrankPattern = re.compile(r'salesrank:\s*(\d+)\n')
    numberOfSimilarsPattern = re.compile(r'similar:\s*(\d+)')
    idsSimilarsPattern = re.compile(r'([\dA-Z]+)')
    
    # Padroes de categoria
    categoriesHeaderPattern = re.compile(r'(?<=categories:)\s*\d+')
    categoriesInfoPattern = re.compile(r'(?<=\|)[a-zA-Z]*\[\d*\]')

    # Padroes de Review
    headerReviewsPattern = re.compile(r'reviews:\stotal:\s(\d+)\s\sdownloaded:\s(\d+)\s\savg rating:\s(\d+\.\d+|\d+)')
    reviewCustomerDate = re.compile(r'\d{4}-\d{1,2}-\d{1,2}')
    reviewCustomerId = re.compile(r'(?<=cutomer:)\s*[\dA-Z]+')
    reviewCustomerRating = re.compile(r'(?<=rating:)\s*\d')
    reviewCustomerVotes = re.compile(r'(?<=votes:)\s*\d+')
    reviewCustomerHelpful = re.compile(r'(?<=helpful:)\s*\d+')

    while i < iMax and linhas[i] != '\n':
        linhaAtual = linhas[i] # p/ debug

        if(elementoAtual == 0):
            elementoAtual += 1
            id = idPattern.findall(linhas[i])
            if(id):
                infoProduto['id'] = id[0]
            else:
                i+=1
                return (None, i) # confirmar decisaso (se nao achar 'id:', significa que nao ha produto)

        elif(elementoAtual == 1):
            elementoAtual += 1
            asin = asinPattern.findall(linhas[i])
            if(asin):
                infoProduto['asin'] = asin[0]
            else:
                infoProduto['asin'] = 'null'
    
        elif(elementoAtual == 2):
            elementoAtual += 1
            title = titlePattern.findall(linhas[i])
            if(title):
                infoProduto['title'] = title[0]
            else:
                infoProduto['title'] = 'null'

        elif(elementoAtual == 3):
            elementoAtual += 1
            group = groupPattern.findall(linhas[i])
            if(group):
                infoProduto['group'] = group[0]
            
            else:
                infoProduto['group'] = 'null'

        elif(elementoAtual == 4):
            elementoAtual += 1
            sales = salesrankPattern.findall(linhas[i])
            if(sales):
                infoProduto['sales'] = sales[0]
            else:
                infoProduto['sales'] = 'null'

        elif(elementoAtual == 5):
            elementoAtual += 1
            numberSimilars = numberOfSimilarsPattern.findall(linhas[i])
            if(len(numberSimilars) > 0):
                numberSimilars = int(numberSimilars[0])
            idsSimilars = idsSimilarsPattern.findall(linhas[i])
            
            if(numberSimilars and idsSimilars):
                infoProduto['similarIds'] = idsSimilars
            else:
                infoProduto['similarIds'] = ['null']

        elif(elementoAtual == 6):
            elementoAtual += 1
            catergoriesHeader = categoriesHeaderPattern.findall(linhas[i])
            if(len(catergoriesHeader) > 0):
                numCatergories = int(catergoriesHeader[0])
                iCategories = 0
                allCategoriesInfo = []
                
                if(numCatergories > 0):
                    while iCategories < numCatergories:
                        i += 1
                        categoriesInfo = nullOrNot(linhas[i])
                        allCategoriesInfo.append(categoriesInfo.replace(' ',''))
                        iCategories += 1

                    infoProduto['categories'] = allCategoriesInfo
                else:
                    infoProduto['categories'] = ['null']
            else:
                infoProduto['categories'] = ['null']
            
        elif(elementoAtual == 7):
            headerReviews = headerReviewsPattern.findall(linhas[i])
            if(headerReviews):
                allReviewInfo = []
                i+=1
                
                try:
                    
                    if(int(headerReviews[0][0]) > 0):
                        while i < iMax and linhas[i] != '\n':
                            rdate = reviewCustomerDate.findall(linhas[i])
                            rid = reviewCustomerId.search(linhas[i])
                            rrating = reviewCustomerRating.search(linhas[i])
                            rvotes = reviewCustomerVotes.search(linhas[i])
                            rhelpful = reviewCustomerHelpful.search(linhas[i])
                            
                            if(rdate):
                                rdate = rdate[0]
                            else:
                                rdate = 'null'
                            if(rid):
                                rid = rid.group(0)
                            else:
                                rid = 'null'
                            if(rrating):
                                rrating = rrating.group(0)
                            else:
                                rrating = 'null'
                            if(rvotes):
                                rvotes = rvotes.group(0)
                            else:
                                rvotes = 'null'
                            if(rhelpful):
                                rhelpful = rhelpful.group(0)
                            else:
                                rhelpful = 'null'

                            reviewAtual = [rdate, rid, rrating, rvotes, rhelpful]
                            allReviewInfo.append(reviewAtual)

                            i+=1
                            #input('debug de dentro leProduto...') # para debug de cada review
                        infoProduto['reviews'] = allReviewInfo
                        #print(f"RESULT. REV: {allReviewInfo}")
                    else:
                        infoProduto['reviews'] = [['null', 'null', 'null','null','null']]
                except:
                    infoProduto['reviews'] = [['null', 'null', 'null','null','null']]
            else:
                infoProduto['reviews'] = [['null', 'null', 'null','null','null']]
            
            break
        i += 1

    i += 1

    if(elementoAtual != 7):
        elementos = ['id', 'asin','title','group','sales', 'similarIds']
        while(elementoAtual <= 7):
            if(elementoAtual < 5):
                infoProduto[elementos[elementoAtual]] = 'null'
            elif(elementoAtual == 5):
                infoProduto['similarIds'] = ['null']
            elif(elementoAtual == 6):
                infoProduto['categories'] = ['null']
            else:
                infoProduto['reviews'] = ['null']
            elementoAtual += 1
    return (infoProduto, i)

def percorreArquivo(arquivo):
    try:
        with open(arquivo, 'r', encoding='utf8') as file:
            linhas = file.readlines()
            totalLinhas = len(linhas)
            i = 0
            while i < totalLinhas:
                produto, i = leProduto(linhas, i, totalLinhas)
                if(produto):
                    if(produto['id']):
                        if(produto['id']%10000 == 0):
                            print("Id atual:", produto['id'], "\n")
                            #printando o id de 10k em 10k para ter noção de progresso
                    insereProduto(produto['id'],produto['asin'],produto['title'],produto['group'],produto['sales'])
                    if(produto['similarIds'][0]!='null'):
                        numSimilares = int(produto['similarIds'][0])
                        x = 1
                        while(x<=numSimilares):
                            insereSimilares(produto['asin'],produto['similarIds'][x])
                            x+=1
                    if(produto['categories'][0] != 'null'):
                        for category in produto['categories']:
                            insereCategorias(produto['asin'],category)
                    if(produto['reviews'] != []):
                        if(produto['reviews'][0] != 'null'):
                            for review in produto['reviews']:
                                insereReviews(review[0],review[1],review[2],review[3],review[4],produto['asin'])

    except FileNotFoundError:
        print(f"Erro: O arquivo '{arquivo}' não foi encontrado.")
    except Exception as e:
        if(produto):
            print(produto)
        print(f"Erro ao ler o arquivo: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso correto: python programa.py <arquivo>")
    else:
        inicializeDB()
        percorreArquivo(sys.argv[1])
