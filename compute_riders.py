import psycopg2
import csv
import geojson
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--route', type=str)
parser.add_argument('--direction', type=str)

args = parser.parse_args()

route = args.route
direction = args.direction

conn = psycopg2.connect(database='transit', user='transit')
cursor = conn.cursor()
cursor.execute("""
select t.shape_id, count(*), min(t.trip_id)
from gtfs_trips t 
join gtfs_calendar c using (service_id)
where monday = '1' and
route_id = %s and
direction = %s
group by 1
order by 2 desc limit 1
""", (route, direction))

results = cursor.fetchone()
shape_id = results[0]
trip_id = results[2]
print shape_id
print trip_id

cursor.execute("""
select
	g.stop_id,
	g.stop_sequence,
	f.boardings,
	f.alightings,
	round(s.stop_lat::numeric, 2),
	round(s.stop_lon::numeric, 2),
	s.stop_name
from
	gtfs_stop_times g
	join foia_data f using (stop_id)
	join gtfs_stops s using (stop_id)
where trip_id = %s
and route = %s
order by g.stop_sequence
""", (trip_id, route))

stops = cursor.fetchall()
print len(stops)

cursor.execute("""
select
	shape_id,
	round(shape_pt_lat::numeric, 2),
	round(shape_pt_lon::numeric, 2),
	round(shape_pt_lat::numeric, 5),
	round(shape_pt_lon::numeric, 5),
	shape_pt_sequence
from gtfs_shapes
where shape_id = %s
order by shape_pt_sequence
""", (shape_id,))
segments = cursor.fetchall()
print len(segments)

stop_index = 0
running_total = 0
old_fine_lat = None
old_fine_lon = None
features = []
osm_ids = set()
#writer = csv.writer(csvfile)
for segment in segments:
	shape_id, coarse_lat, coarse_lon, fine_lat, fine_lon, segment_sequence = segment
	if stop_index > len(stops)-1:
		continue
	if str(coarse_lat) == str(stops[stop_index][4]) and str(coarse_lon) == str(stops[stop_index][5]):
		running_total += stops[stop_index][2]
		running_total -= stops[stop_index][3]
		print running_total, "on the bus at", stops[stop_index][6], fine_lat, fine_lon
		stop_index += 1
	if old_fine_lat is not None:
		print running_total, "on between", old_fine_lat, old_fine_lon, "and", fine_lat, fine_lon
		cursor.execute("""
			select osm_id, st_asgeojson(st_transform(way, 4326))
			from (select st_geographyfromtext('srid=4326;linestring(%s %s,%s %s)') as s) shape
			join planet_osm_line on (st_dwithin(shape.s, st_transform(way, 4326), 10) and highway not in ('footway', ''))
			order by anglediff(shape.s::geometry, st_transform(way, 4326)::geometry) asc
		""", (old_fine_lon, old_fine_lat, fine_lon, fine_lat)) 
		osm_matches = cursor.fetchall()
		if len(osm_matches) == 0:
			print "none!"
			#x = 1/0
		for osm_match in osm_matches:
			if osm_match[0] in osm_ids:
				print "skipping because osm id", osm_match[0], "already found"
				break	
			feature = geojson.Feature(
				geometry=geojson.loads(osm_match[1]),
				properties={
					'num_passengers': round(running_total, 1),
					'osm_id': osm_match[0]
				},
			)
			print "found osm id", osm_match[0], "as a match"
			osm_ids.add(osm_match[0])
			features.append(feature)
			break
		#writer.writerow([old_fine_lat, old_fine_lon, fine_lat, fine_lon, round(running_total, 1)])
	old_fine_lat = fine_lat
	old_fine_lon = fine_lon

coll = geojson.FeatureCollection(features)
with open('data/{}_{}_segments.geojson'.format(route, direction), 'wb') as jsonfile:
	jsonfile.write(geojson.dumps(coll))
