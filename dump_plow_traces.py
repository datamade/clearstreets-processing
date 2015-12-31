if __name__ == "__main__":
    from processors.tracer import Tracer
    import sqlalchemy as sa
    from processors.config import DB_CONN
    import itertools
    from datetime import datetime

    engine = sa.create_engine(DB_CONN)

    plows = ''' 
        SELECT object_id
        FROM route_points
        GROUP BY object_id
        HAVING (count(*) > 1000)
    '''
    
    plow_ids = [r.object_id for r in engine.execute(plows)]


    sigmas = [5, 10, 15, 20, 30]
    betas = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    
    for sigma, beta in itertools.product(sigmas, betas):
        
        clear_plows = ''' 
            UPDATE route_points SET
              inserted = FALSE
            WHERE object_id IN :plow_ids
        '''

        with engine.begin() as conn:
            conn.execute(sa.text(clear_plows), plow_ids=tuple(plow_ids))
        
        tracer = Tracer(plow_ids=plow_ids)
        tracer.gps_precision = sigma
        tracer.matching_beta = beta

        not_processed_q = ''' 
            SELECT COUNT(*) AS count
            FROM route_points 
            WHERE object_id IN :plow_ids 
              AND inserted = FALSE
        '''
        
        not_processed = engine.execute(sa.text(not_processed_q), 
                                       plow_ids=tuple(plow_ids)).first().count
        
        remainder = []

        while True:
            tracer.dumpGeoJSON()
            
            not_processed = engine.execute(sa.text(not_processed_q), 
                                           plow_ids=tuple(plow_ids)).first().count
            
            if not_processed in remainder:
                print('starting next loop')
                break

            remainder.append(not_processed)

            print('{} left to process'.format(not_processed))

