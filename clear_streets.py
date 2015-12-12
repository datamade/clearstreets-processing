
if __name__ == "__main__":
    import argparse
    import sys
    from processors.slurper import Slurper
    from processors.tracer import Tracer

    parser = argparse.ArgumentParser(description='Clear the streets')
    parser.add_argument('--slurp', action='store_true',
                   help='Start slurping the feed')
    
    parser.add_argument('--test_mode', action='store_true',
                   help='Run slurper against test data')
    
    parser.add_argument('--recreate_tables', action='store_true',
                   help='Recreate trace and asset tables before slurping')
    
    parser.add_argument('--write_cartodb', action='store_true',
                   help='Write slurped traces to CartoDB')
    
    parser.add_argument('--backup', action='store_true',
                   help='Backup database contents to S3')

    args = parser.parse_args()
    
    if args.slurp and args.write_cartodb:
        print('Cannot slurp from city and write to CartoDB in the same process')
        sys.exit()

    if args.slurp:
        slurper = Slurper(test_mode=args.test_mode)
        slurper.run(recreate=args.recreate_tables)
    
    if args.write_cartodb:
        tracer = Tracer()

        while True:
            tracer.run()
            time.sleep(10)

    if args.backup:
        from datetime import datetime
        import sqlalchemy as sa
        
        import boto
        from boto.s3.key import Key
        
        from processors.config import DB_CONN, AWS_KEY, AWS_SECRET, S3_BUCKET
        
        engine = sa.create_engine(DB_CONN)

        conn = engine.raw_connection()

        s3conn = boto.connect_s3(AWS_KEY, AWS_SECRET)
        bucket = s3conn.get_bucket(S3_BUCKET)

        for table in ['route_points', 'assets']:
            copy = ''' 
                COPY (SELECT * FROM {table})
                TO STDOUT WITH CSV HEADER DELIMITER ','
            '''.format(table=table)
            
            now = datetime.now().strftime('%m-%d-%Y_%H:%M')
            
            fname = 'backups/{now}_{table}.csv'.format(now=now, 
                                                       table=table)
            
            with open(fname, 'w') as f:
                curs = conn.cursor()
                curs.copy_expert(copy, f)
            
            key = Key(bucket)
            key.key = fname
            key.set_contents_from_filename(fname)
            key.set_acl('public-read')

        conn.close()
        s3conn.close()
