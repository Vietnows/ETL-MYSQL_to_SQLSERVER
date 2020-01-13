
import pyodbc
import pandas as pd


serverBI = '##########'
userBI= '##########'
passwBI = '##########'
databaseBI = '##########'
driverBI = '{ODBC Driver 17 for SQL Server}'

# --- Magento  MySQL S2G ----#
hostS2G = '##########'
userS2G = '##########'
passwS2G = '##########'
databaseS2G = '##########'
driverMySQL = '{MySQL ODBC 8.0 Unicode Driver}'

# --- Magento  MySQL OQV---#
hostOQV = '##########'
userOQV = '##########'
passwOQV =  '##########'
databaseOQV = '##########'

#Configs para ICOMMBI SQL SERVER
connectionBI = pyodbc.connect('DRIVER='+ driverBI+';SERVER='+serverBI+';DATABASE='+databaseBI+';UID='+userKallas+';PWD='+passwKallas)
cursor = connectionBI.cursor()

#Configs para Magento mysql
connectionS2G = pyodbc.connect('DRIVER='+ driverMySQL+';SERVER='+hostS2G+';DATABASE='+databaseS2G+';UID='+userS2G+';PWD='+passwS2G)
connectionOQV = pyodbc.connect('DRIVER='+ driverMySQL+';SERVER='+hostOQV+';DATABASE='+databaseOQV+';UID='+userOQV+';PWD='+passwOQV)

queryS2G = "select 'S2G' as canal,produtos.sku as produto,produtos_pai.sku as produto_pai,nome_produto.value descricao,nome_colecao.value as colecao,nome_marca.value as marca,nome_genero.value genero,aviseme.* from product_alert_stock aviseme left join catalog_product_entity produtos on aviseme.product_id = produtos.entity_id left join catalog_product_entity produtos_pai on aviseme.parent_id = produtos_pai.entity_id left join catalog_product_entity_int colecao on produtos.entity_id = colecao.entity_id left join eav_attribute_option_value nome_colecao on colecao.value = nome_colecao.option_id left join catalog_product_entity_int marca on produtos.entity_id = marca.entity_id left join eav_attribute_option_value nome_marca on marca.value = nome_marca.option_id left join catalog_product_entity_varchar nome_produto on produtos.entity_id = nome_produto.entity_id left join catalog_product_entity_int genero on produtos.entity_id =  genero.entity_id left join eav_attribute_option_value nome_genero on genero.value = nome_genero.option_id where colecao.attribute_id = 228 and marca.attribute_id = 239 and nome_produto.attribute_id = 65 and genero.attribute_id = 182 and date(aviseme.add_date) = subdate(curdate(), 1)" 
queryOQV = "select 'OQV' as canal,produtos.sku as produto,produtos_pai.sku as produto_pai,nome_produto.value descricao,nome_colecao.value as colecao,nome_marca.value as marca,'FEMININO' as genero,aviseme.* from product_alert_stock aviseme left join catalog_product_entity produtos on aviseme.product_id = produtos.entity_id left join catalog_product_entity produtos_pai on aviseme.parent_id = produtos_pai.entity_id left join catalog_product_entity_int colecao on produtos.entity_id = colecao.entity_id left join eav_attribute_option_value nome_colecao on colecao.value = nome_colecao.option_id left join catalog_product_entity_int marca on produtos.entity_id = marca.entity_id left join eav_attribute_option_value nome_marca on marca.value = nome_marca.option_id left join catalog_product_entity_varchar nome_produto on produtos.entity_id = nome_produto.entity_id where colecao.attribute_id = 228 and marca.attribute_id = 239 and nome_produto.attribute_id = 65 and date(aviseme.add_date) = subdate(curdate(), 1)"

#Select dados Magento
print('Querying Magento MySQL')
resultOQV = pd.read_sql_query(queryOQV,connectionOQV)
resultS2G = pd.read_sql_query(queryS2G,connectionS2G)


#tratando os dados no Pandas Python
#df_all['add_date'] = pd.to_datetime(df_all['add_date'])
dfs = [resultOQV, resultS2G]
df_all = pd.concat(dfs,ignore_index=True)
df_all.drop('send_date', axis=1,inplace=True)
df_all = df_all.fillna(0)
df_all['store_id'] = df_all['store_id'].apply(int) 

#Inserindo Dados no Icommbi
print('insert data ICOMMBI ')
for index, row in df_all.iterrows():
    cursor.execute("INSERT INTO AVISE_ME(canal,produto,produto_pai,descricao,colecao,marca,genero,alert_stock,customer_id,product_id,website_id,send_count,status,parent_id,email,store_id,add_date) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                   , row['canal'],row['produto'],row['produto_pai'],row['descricao'],row['colecao'],row['marca'],row['genero'],
                     row['alert_stock_id'],row['customer_id'],row['product_id'],row['website_id'], row['send_count'],row['status'],
                     row['parent_id'],row['email'],row['store_id'],row['add_date']
                  ) 
    
connectionBI.commit()    
connectionBI.close()
connectionS2G.close()
connectionOQV.close()

print('Finished')