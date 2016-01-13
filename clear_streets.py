
if __name__ == "__main__":
    import argparse
    import sys
    from processors.slurper import Slurper, TestSlurper
    from processors.tracer import Tracer
    import logging
    import time

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

    parser.add_argument('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)')

    args = parser.parse_args()

    log_level = logging.WARNING 
    if args.verbose :
        if args.verbose == 1:
            log_level = logging.INFO
        elif args.ve >= 2:
            log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    if args.slurp and args.write_cartodb:
        print('Cannot slurp from city and write to CartoDB in the same process')
        sys.exit()

    if args.slurp:
        if args.test_mode :
            slurper = TestSlurper()
        else :
            slurper = Slurper()
        slurper.run(recreate=args.recreate_tables)
    
    if args.write_cartodb:
        tracer = Tracer(test_mode=args.test_mode)

        while True:
            tracer.run()
            time.sleep(10)

    if args.backup:
        slurper = Slurper()
        slurper.backup()



