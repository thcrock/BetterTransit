-- nodes processing, called from OSRM
function node_function(node)
  return 1
end

local file = io.open("../gtfs_ways.txt", "r");
local arr = {}
for line in file:lines() do
	arr[line] = true
end

-- ways processing, called from OSRM
function way_function (way, result)
  local way_id = '' .. way:id()
  if arr[way_id] then
	result.forward_speed = 50.0
	result.forward_mode = mode.bus
	result.backward_speed = 50.0
	result.backward_mode = mode.bus
	return 1
  else
	return 0
  end
end
