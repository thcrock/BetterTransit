CREATE OR REPLACE FUNCTION angleDiff (l1 geometry, l2 geometry)
RETURNS FLOAT AS $$
DECLARE angle1 FLOAT;
DECLARE angle2 FLOAT;
DECLARE diff FLOAT;
BEGIN           
    SELECT ST_Azimuth (ST_StartPoint(l1), ST_EndPoint(l1)) INTO angle1;
    SELECT ST_Azimuth (ST_StartPoint(l2), ST_EndPoint(l2)) INTO angle2;  
    SELECT degrees (angle2 - angle1) INTO diff;
    CASE
    WHEN diff >   180 THEN RETURN diff - 360;
    WHEN diff <= -180 THEN RETURN diff + 360;
    ELSE                   RETURN diff;
    END CASE;
END;
$$  LANGUAGE plpgsql
