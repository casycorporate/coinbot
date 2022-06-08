import psycopg2
import  config


class DBObject():
    def __init__(self):
        self.conn=psycopg2.connect(
            database=config.DATABASE_CONFIG['database'],
            user=config.DATABASE_CONFIG['user'],
            password=config.DATABASE_CONFIG['password'],
            host=config.DATABASE_CONFIG['host'],
            port=config.DATABASE_CONFIG['port']
        )
        # establishing the connection
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()


    def createCoinListTable(self):
        # EXECUTE THE INSERT QUERY
        self.cursor.execute(f'''
             CREATE TABLE IF NOT EXISTS  public.coinList (
             symbol text NOT NULL,
             PRIMARY KEY (symbol)
         ) ''')



    def createSchema(self,schemanName='''mexc'''):
        sql = '''CREATE SCHEMA IF NOT EXISTS %s''' % schemanName;
        self.cursor.execute(sql)

    def insert_value(self,timestmp, open_, close_, high, low, vol, amount, tablename):
        # CREATE A CURSOR USING THE CONNECTION OBJECT


        # EXECUTE THE INSERT QUERY
        self.cursor.execute(f'''
            INSERT INTO
                mexc.{tablename}(time_, open_,close_,high,low,vol,amount) 
            VALUES
                ('{timestmp}', '{open_}', '{close_}', '{high}', '{low}', '{vol}', '{amount}')
        ''')

    def insert2_coinList(self,symbol):
        # CREATE A CURSOR USING THE CONNECTION OBJECT

        # EXECUTE THE INSERT QUERY
        self.cursor.execute(f'''
               INSERT INTO
                   public.coinList(symbol) 
               VALUES
                   ('{symbol}')
           ''')

        # COMMIT THE ABOVE REQUESTS

    #Preparing query to create a database
    def getAllTableList(self, schema="""mexc"""):
        self.cursor.execute("""SELECT * FROM public.coinList""")
        tables = [i[0] for i in self.cursor.fetchall()]
        return tables

    def create_table(self,tableName):

        print(tableName)
        # EXECUTE THE INSERT QUERY
        self.cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS  mexc.{tableName} (
        time_ timestamp NULL,
        open_ float8 NULL,
        close_ float8 NULL,
        high float8 NULL,
        low float8 NULL,
        vol float8 NULL,
        amount float8 NULL
    ) ''')

    def connClose(self):
        self.cursor.close()
        self.conn.close()
