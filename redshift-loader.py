import yaml,os,psycopg2



file_to_table_mapping = {
    "address":"default.dim_address",
    "flats":"default.fact_flat",
    "agency":"default.dim_agency"
}
def handler(event, context):
    if event:
        try:
            with open(os.environ['CONFIG_FILE'], 'r') as stream:
                config = yaml.load(stream)
                file_obj = event['Records'][0]
                bucket_name = file_obj['s3']['bucket']['name']
                key = file_obj['s3']['object']['key']

                conn = psycopg2.connect(
                host=config['host'],
                user=config['user'],
                port=config['port'],
                password=config['password'],
                dbname='analytics')
                table_name = ''
                if "address" in key:
                    table_name = file_to_table_mapping.get("address")
                elif "flats" in key:
                    table_name = file_to_table_mapping.get("flats")
                else:
                    table_name = file_to_table_mapping.get("agency")   

                s3_file = 's3://'+bucket_name+'/'+key
                copy_sql =('COPY ' +table_name+' from \''+s3_file+'\''+
                    ' CREDENTIALS '+
                        'aws_access_key_id='+os.environ['AWS_ACCESS_KEY']+';aws_secret_access_key='+os.environ['AWS_SECRET_KEY']+'\''
                            + ' csv'
                    + ' gzip'
                    + ' ignoreheader 1'
                    + ' delimiter as \'\\t\''
                    + ' truncatecolumns'
                    + ' ignoreblanklines'
                    + ' statupdate on'
                    + ' maxerror 0' ) 
                cur = conn.cursor()
                cur.execute(copy_sql)
                conn.commit()
                return
        except Exception as e:
            print("Exception occured while loading data into redshift"+e) 
            return
    return        
                
