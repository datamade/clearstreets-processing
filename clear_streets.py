
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
