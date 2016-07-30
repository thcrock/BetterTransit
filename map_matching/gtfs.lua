
-- This example shows how to query external data stored in PostGIS when processing ways.

-- This profile assumes that OSM data has been imported to PostGIS using imposm to a db
-- with the name 'imposm', the default user and no password. It assumes  areas with
-- landusage=* was imported to the table osm_landusages, containting the columns type and area.
-- Seee http://imposm.org/ for more info on imposm.
-- Other tools for importing OSM data to PostGIS include osm2pgsql and osmosis.

-- It uses the PostGIS function ST_DWithin() to find areas tagged with landuse=industrial
-- that are within 100 meters of the way.
-- It then slows down the routing depending on the number and size of the industrial area.

-- The end result is that routes will tend to avoid industrial area. Passing through
-- industrial areas is still possible, it's just slower, and thus avoided if a reasonable
-- alternative is found.

-- We use the osm id as the key when querying PostGIS. Be sure to add an index to the colunn
-- containing the osm id (osm_id in this case), otherwise you will suffer form very
-- bad performance. You should also have spatial indexes on the relevant gemoetry columns.

-- More info about using SQL form LUA can be found at http://www.keplerproject.org/luasql/

-- Happy routing!


-- Open PostGIS connection
lua_sql = require "luasql.postgres"           -- we will connect to a postgresql database
sql_env = assert( lua_sql.postgres() )
sql_con = assert( sql_env:connect("transit", "transit", "transit", "127.0.0.1", 5432) ) -- you can add db user/password here if needed
print("PostGIS connection opened")

-- these settings are read directly by osrm
take_minimum_of_speeds   = true
obey_oneway             = true
use_restrictions         = true
ignore_areas             = true  -- future feature
traffic_signal_penalty   = 7      -- seconds
u_turn_penalty           = 20

-- nodes processing, called from OSRM
function node_function(node)
  return 1
end

-- ways processing, called from OSRM
function way_function (way, result)
  -- only route on ways with highway=*
  local highway = way.tags:Find("highway")
  if (not highway or highway=='') then
    return 0
  end

  local sql_query = "SELECT 1 from gtfs_ways where way_id = " .. way.id

  local cursor = assert( sql_con:execute(sql_query) )
  local numrows = cursor:numrows()
  cursor:close()

  if (numrows == 0) then
	  return 0
  end

  -- set other required info for this way
  result.name = way.get_value_by_key("name")
  return 1
end
