:while1
  "c:\Program Files (x86)\GnuWin32\bin\wget.exe" -r -l1 --no-parent -A.gpx -nd -N -P gpx http://bunkum.us/gpx/
  MatchGPX2OSM.exe --osm=travel_time\chicago-r.osm --gpx=gpx --output=osm
  CHOICE /C 1 /D 1 /T 30 > nul
goto :while1