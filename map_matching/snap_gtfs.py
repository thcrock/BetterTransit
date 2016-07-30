from sqlalchemy import (
    BigInteger,
    Column,
    create_engine,
    Integer,
    MetaData,
    String,
    Table,
    Text
)
from snap_points import snap


def ensure_schema(engine):
    meta = MetaData()
    Table(
        'gtfs_node_pairs',
        meta,
        Column('shape_id', String),
        Column('begin_node', BigInteger),
        Column('end_node', BigInteger),
        Column('num_trips', Integer),
        Column('routes', String),
    )
    Table(
        'gtfs_reduced_node_pairs',
        meta,
        Column('node_one', BigInteger),
        Column('node_two', BigInteger),
        Column('num_trips', Integer),
        Column('routes', String),
        Column('shape_ids', String),
    )
    Table(
        'gtfs_ways',
        meta,
        Column('begin_node', BigInteger),
        Column('end_node', BigInteger),
        Column('way_id', BigInteger),
        Column('num_trips', Integer),
        Column('routes', String),
        Column('shapes', String),
        Column('geometry_geojson', Text),
    )
    meta.create_all(engine)

def snap_gtfs(engine):
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute('truncate gtfs_node_pairs')
    cursor.execute("""
    select
        shape_id,
        array_agg(shape_pt_lat order by shape_pt_sequence asc) as lats,
        array_agg(shape_pt_lon order by shape_pt_sequence asc) as lons,
        max(num_trips) as num_trips,
        max(routes) as routes
    from gtfs_shapes
    join (
        select
            distinct(shape_id) as shape_id,
            count(distinct trip_id) as num_trips,
            string_agg(distinct route_id, ',') as routes
        from
            gtfs_trips
            join gtfs_calendar on (gtfs_trips.service_id = gtfs_calendar.service_id and tuesday = 1)
            where route_id not in ('Blue', 'Red', 'Brn', 'P', 'Y', 'Pink', 'Org', 'G')
            group by 1
    ) distinct_shapes using (shape_id)
    group by shape_id
    """)
    result = cursor.fetchall()
    for shape_id, lats, lons, num_trips, routes in result:
        coord_string = ';'.join(
            "%s,%s" % (lon, lat) for lat, lon in zip(lats, lons)
        )
        output = snap(coord_string, ['20' for _ in lats])
        shape_nodes = []
        if 'tracepoints' in output:
            for lat, lon, tracepoint in zip(
                lats,
                lons,
                output['tracepoints']
            ):
                if tracepoint:
                    match = output['matchings'][tracepoint['matchings_index']]
                    legs = match['legs']
                    if tracepoint['waypoint_index'] == len(legs):
                        continue
                    leg = legs[tracepoint['waypoint_index']]
                    nodes = leg['annotation']['nodes']
                    for node in nodes:
                        if node not in shape_nodes:
                            shape_nodes.append(node)
            for i in range(0, len(shape_nodes)-1, 1):
                cursor.execute(
                    'insert into gtfs_node_pairs values (%s, %s, %s, %s, %s)',
                    (shape_id, shape_nodes[i], shape_nodes[i+1], num_trips, routes)
                )
    connection.commit()

def reduce_node_pairs(engine):
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute('select * from gtfs_node_pairs')
    node_pairs = cursor.fetchall()
    squashed_pairs = {}
    for shape_id, begin_node, end_node, num_trips, route in node_pairs:
        entry = None
        if (end_node, begin_node) in squashed_pairs:
            entry = (end_node, begin_node)
        else:
            entry = (begin_node, end_node)
        if entry not in squashed_pairs:
            squashed_pairs[entry] = {
                'num_trips': 0,
                'routes': set(),
                'shape_ids': set()
            }
        squashed_pairs[entry]['num_trips'] += num_trips
        squashed_pairs[entry]['routes'].add(route)
        squashed_pairs[entry]['shape_ids'].add(shape_id)

    cursor.execute('truncate gtfs_reduced_node_pairs')
    for key in squashed_pairs:
        row = squashed_pairs[key]
        node_one, node_two = key
        cursor.execute(
            'insert into gtfs_reduced_node_pairs values (%s, %s, %s, %s, %s)',
            (node_one, node_two, row['num_trips'], list(row['routes']), list(row['shape_ids']))
        )
    connection.commit()

def compute_ways(engine):
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute('truncate gtfs_ways')
    query = """
        insert into gtfs_ways
	select
            node_one,
            node_two,
            max(planet_osm_ways.id) as way_id,
            sum(num_trips) as num_trips,
            string_agg(distinct routes, ',') as routes,
            string_agg(distinct shape_ids, ',') as shapes,
           st_asgeojson(st_makeline(
                   st_transform(
                           st_geomfromtext('point('||bnode.lon/100||' '||bnode.lat/100||')', 3785),
                           4326
                   ),
                   st_transform(
                           st_geomfromtext('point('||enode.lon/100||' '||enode.lat/100||')', 3785),
                           4326
                   )
           )) as geometry_geojson
	from
		gtfs_reduced_node_pairs
		join planet_osm_ways on (nodes @> ARRAY[node_one] and nodes @> ARRAY[node_two])
		left join planet_osm_nodes bnode on (node_one = bnode.id)
		left join planet_osm_nodes enode on (node_two = enode.id)
	group by 1, 2, 7
	order by 4 desc"""
    cursor.execute(query)
    connection.commit()


def export(engine):
    table = 'gtfs_ways'
    output_filename = "{}.csv".format(table)
    with open(output_filename, 'wb') as f:
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(
            sql='COPY {} to stdout with csv header'.format(table),
            file=f
        )

if __name__ == '__main__':
    engine = create_engine("postgresql://transit:transit@127.0.0.1/transit")
    ensure_schema(engine)
    snap_gtfs(engine)
    reduce_node_pairs(engine)
    compute_ways(engine)
    export(engine)
