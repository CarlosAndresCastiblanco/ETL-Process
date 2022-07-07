from pyspark import SparkContext, SparkConf, SQLContext
from datetime import datetime
from pyspark.sql.functions import  when
from pyspark.sql import functions as f, types as t

# Configuración del driver en el path local
path_jar_driver = 'C:\Workspaces\mysql-connector-java-8.0.29.jar'

# Configuración de la sesión de pyspark
conf=SparkConf() \
    .set('spark.driver.extraClassPath', path_jar_driver)

spark_context = SparkContext(conf=conf)
sql_context = SQLContext(spark_context)

# Inicio de la sesión
spark = sql_context.sparkSession

# Metodo de consulta de datos
def obtener_dataframe_de_bd(db_connection_string, sql, db_user, db_psswd):
    df_bd = spark.read.format('jdbc')\
        .option('url', db_connection_string) \
        .option('dbtable', sql) \
        .option('user', db_user) \
        .option('password', db_psswd) \
        .option('driver', 'com.mysql.cj.jdbc.Driver') \
        .load()
    return df_bd

# Metodo de guardado de datos
def guardar_db(db_connection_string, df, tabla, db_user, db_psswd):
    df.select('*').write.format('jdbc') \
      .mode('append') \
      .option('url', db_connection_string) \
      .option('dbtable', tabla) \
      .option('user', db_user) \
      .option('password', db_psswd) \
      .option('driver', 'com.mysql.cj.jdbc.Driver') \
      .save()

######### Definición de credenciales ################
db_user = 'Estudiante_14'
db_psswd = 'KGRE7I6YIZ'
#####################################################

# Definición de los esquemas de consulta y persistencia
source_db_connection_string  = 'jdbc:mysql://157.253.236.116:8080/WWImportersTransactional'
dest_db_connection_string = 'jdbc:mysql://157.253.236.116:8080/Estudiante_14'

## PROVEEDOR ##

# Extracción de Proveedores y categoria proveedores y transformación de los label de las columnas requeridas
sql_proveedor = '''(SELECT ProveedorID AS ID_Proveedor, NombreProveedor AS Nombre, NumeroTelefono AS Contacto_principal , DiasPago AS Dias_pago , CodigoPostal AS Codigo_postal, CategoriaProveedorID AS ID_Categoria FROM WWImportersTransactional.Proveedores) AS Temp_proveedor'''
sql_categoria_proveedor = '''(SELECT CategoriaProveedor AS Categoria, CategoriaProveedorID AS ID_Categoria FROM WWImportersTransactional.CategoriasProveedores) AS Temp_categoria_proveedor'''
proveedor = obtener_dataframe_de_bd(source_db_connection_string, sql_proveedor, db_user, db_psswd)
categoria_proveedor = obtener_dataframe_de_bd(source_db_connection_string, sql_categoria_proveedor, db_user, db_psswd)
print("Extracción de Proveedores........................................")
proveedor.show(5)
print("Extracción de Categoria Proveedor........................................")
categoria_proveedor.show(5)

# Transformación de datos por medio de un Join para asociar las categorias
proveedor = proveedor.join(categoria_proveedor, how='inner', on = 'ID_Categoria')
print("Transformación de datos Proveedor........................................")
proveedor.show(5)

# Eliminación de la columna ID_Categoria antes de guardar la data
proveedor = proveedor.drop('ID_Categoria')
print("Eliminación de datos Proveedor........................................")
proveedor.show(5)

# Ordenamiento segun el modelo planteado
order_proveedor = proveedor.select("ID_Proveedor","Nombre","Categoria","Contacto_principal","Dias_pago","Codigo_postal")
print("Ordenamiento de datos Proveedor........................................")
order_proveedor.show(5)

# Correción de negativos y eliminación de duplicados
correct_proveedor = order_proveedor.withColumn("Dias_pago", when(order_proveedor.Dias_pago < 0,order_proveedor.Dias_pago * -1).otherwise(order_proveedor.Dias_pago))
correct_proveedor = correct_proveedor.drop_duplicates()
print("Corrección de negativos en proveedores........................................")
correct_proveedor.show(5)

# Load - Carga de los datos proveedor
guardar_db(dest_db_connection_string, correct_proveedor,'Estudiante_14.Proveedor', db_user, db_psswd)

## TIPO DE TRANSACCION ##

# Extraccion y transformación de datos de tipo transaccion y eliminación de duplicados
sql_tipo_transaccion = '''(SELECT TipoTransaccionID AS ID_Tipo_transaccion, TipoTransaccionNombre AS Tipo FROM WWImportersTransactional.TiposTransaccion) AS Temp_tipo_transaccion'''
tipo_transaccion = obtener_dataframe_de_bd(source_db_connection_string, sql_tipo_transaccion, db_user, db_psswd)
tipo_transaccion = tipo_transaccion.drop_duplicates()
print("Extraccion de tipo_transaccion........................................")
tipo_transaccion.show(5)

# Load - Carga de los datos para tipo transacción
guardar_db(dest_db_connection_string, tipo_transaccion,'Estudiante_14.TipoTransaccion', db_user, db_psswd)

## PRODUCTO ##

# Extraccion Producto
sql_producto = '''(SELECT ID_Producto AS ID_Producto,NombreProducto AS Nombre,Marca AS Marca,ID_Color AS ID_Color,Necesita_refrigeracion AS Necesita_refrigeracion,Dias_tiempo_entrega AS Dias_tiempo_entrega,PrecioRecomendado AS Precio_minorista_recomendado,PrecioUnitario AS Precio_unitario FROM WWImportersTransactional.Producto) AS Temp_producto'''
sql_color = '''(SELECT ID_Color AS ID_Color, Color as Color FROM WWImportersTransactional.Colores) AS Temp_colores'''
producto = obtener_dataframe_de_bd(source_db_connection_string, sql_producto, db_user, db_psswd)
color = obtener_dataframe_de_bd(source_db_connection_string, sql_color, db_user, db_psswd)
print("Extraccion Producto........................................")
producto.show(5)
print("Extraccion Color........................................")
color.show(5)

# Transformación de datos producto con inner de color
producto = producto.join(color, how='inner', on = 'ID_Color')
print("Transformación de datos Color........................................")
producto.show(5)

# Eliminación de la columna ID_Color antes de guardar la data
producto = producto.drop('ID_Color')
print("Eliminación de la columna ID_Color........................................")
producto.show(5)

# Ordenamiento segun el modelo planteado y eliminación de duplicados
order_producto = producto.select("ID_Producto","Nombre","Marca","Color","Necesita_refrigeracion","Dias_tiempo_entrega","Precio_minorista_recomendado","Precio_unitario")
order_producto.drop_duplicates()
print("Ordenamiento producto........................................")
order_producto.show(5)

# Load - Carga de los datos Producto
guardar_db(dest_db_connection_string, order_producto,'Estudiante_14.Producto', db_user, db_psswd)

## CLIENTE ##

# Extraccion Cliente
sql_cliente = '''(SELECT ID_Categoria, ID_GrupoCompra, ID_Cliente, Nombre, ClienteFactura, ID_CiudadEntrega, LimiteCredito, FechaAperturaCuenta, DiasPago FROM WWImportersTransactional.Clientes) AS Temp_cliente'''
sql_grupo_compra = '''(SELECT ID_GrupoCompra ,NombreGrupoCompra FROM WWImportersTransactional.GruposCompra) AS Temp_grupo_compra'''
sql_categoria = '''(SELECT ID_Categoria ,NombreCategoria FROM WWImportersTransactional.CategoriasCliente) AS Temp_categoria'''
cliente = obtener_dataframe_de_bd(source_db_connection_string, sql_cliente, db_user, db_psswd)
grupo_compra = obtener_dataframe_de_bd(source_db_connection_string, sql_grupo_compra, db_user, db_psswd)
categoria = obtener_dataframe_de_bd(source_db_connection_string, sql_categoria, db_user, db_psswd)

# Transformación de datos cliente con inner de grupo compra y categoria
cliente = cliente.join(grupo_compra, how='inner', on = 'ID_GrupoCompra')
cliente = cliente.join(categoria, how='inner', on = 'ID_Categoria')

# Ordenamiento segun el modelo planteado y eliminación de duplicados
order_cliente = cliente.select("ID_Categoria","ID_GrupoCompra","ID_Cliente","Nombre","ClienteFactura","ID_CiudadEntrega","LimiteCredito","FechaAperturaCuenta","DiasPago")
order_cliente.drop_duplicates()
print("Ordenamiento cliente........................................")
order_cliente.show(5)

# Load - Carga de los datos Cliente
guardar_db(dest_db_connection_string, order_cliente,'Estudiante_14.Cliente', db_user, db_psswd)

## HECHO MOVIMIENTO ##

# Extraccion Movimientos
sql_movimientos = '''(SELECT TransactionOccurredWhen AS Fecha_movimiento, StockItemID AS ID_Producto, SupplierID AS ID_proveedor, CustomerID AS ID_Cliente, TransactionTypeID AS ID_Tipo_transaccion, Quantity AS Cantidad FROM WWImportersTransactional.movimientos) AS Temp_movimientos'''
movimientos = obtener_dataframe_de_bd(source_db_connection_string, sql_movimientos, db_user, db_psswd)

# TRANSFORMACION

# Estandarización de formato fecha para movimientos y eliminación de duplicados
regex = "([0-2]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]))"
cumplenFormato = movimientos.filter(movimientos["Fecha_movimiento"].rlike(regex))
noCumplenFormato = movimientos.filter(~movimientos["Fecha_movimiento"].rlike(regex))
print("noCumplenFormato..vs..cumplenFormato..................")
print(noCumplenFormato.count(), cumplenFormato.count())
print("noCumplenFormato......................................")
noCumplenFormato.show(5)
noCumplenFormato = noCumplenFormato.withColumn('Fecha_movimiento', f.udf(lambda d: datetime.strptime(d, '%b %d,%Y').strftime('%Y-%m-%d'), t.StringType())(f.col('Fecha_movimiento')))
print("movimientos......................................")
movimientos.drop_duplicates()
movimientos.show(5)

# Load - Carga de los movimientos
inferior = 0
superior = 999
j=0
total = movimientos.count()/1000
print(total)
collected = movimientos.collect()
print("Inicio de guardado Movimientos......................................")
while j<total:
    if j%50==0:
        print(j)
    j += 1
    aux = spark.createDataFrame(collected[inferior:superior],movimientos.columns)
    guardar_db(dest_db_connection_string, aux,'Estudiante_14.Hecho_Movimiento', db_user, db_psswd)
    inferior+=1000
    superior+=1000
print("Final del proceso......................................")
