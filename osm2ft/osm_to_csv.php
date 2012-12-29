<?php
  //see README for instructions
  error_reporting(E_ALL);
  ini_set("display_startup_errors",1);
  ini_set("display_errors",1);
  ini_set("memory_limit","200M"); //datasets we are dealing with can be quite large, need enough space in memory
  set_time_limit(0);
  date_default_timezone_set('America/Chicago');
  
  header('Content-type: text/plain');
  echo "Plow Tracker CSV import by Derek Eder\n\n";
  
  //Fetch data from Forest's OSM files
  chdir ("../osm/");
  $current_dir = getCwd();
  echo "Current directory is now $current_dir";
  
  $array = scandir(".", 1);
  print_r($array);
  $bgtime=time();
  
  $current_time;
  $insertCount = 0;
  echo "\n----Inserting in to CSV----\n";
  $fp = fopen('clearstreets.csv', 'w+');
  
  foreach($array as $filename) 
  {
  	$plowID = "";
  	$datestamp = "";
  	$geo_to_insert = array();
  	
  	if ($filename != "." && $filename != ".." && $filename != ".DS_Store") {
  	  //echo "$filename\n";
	  @$xml = simplexml_load_file("$filename");
	  
	  if ($xml != null) {
	  foreach ($xml->xpath('/osm/node') as $node) {
	  	$primary_attr = $node->attributes();   // returns an array
	    //echo strstr($filename, '_', true) . ' ' . $primary_attr['lat'] . ' ' . $primary_attr['lon'] . PHP_EOL;
	    
	    foreach ($node->children() as $tag) 
		{
	    	$secondary_attr = $tag->attributes();
	    	if ($secondary_attr['k'] == 'time') 
			{
	    		//if we come across a time, we know to insert the current set of collected values
				if (!empty($geo_to_insert))
				{
					$geo_to_insert[] = $primary_attr['lon'] . ',' . $primary_attr['lat'];
					save_to_csv($plowID, $datestamp, $geo_to_insert, $fp);
					$insertCount++;
				}
					
	    		$geo_to_insert = array();
	    		$current_time = $secondary_attr['v'];
	    	}
	    }
	    
	    $plowID = strstr($filename, '_', true);
	    $datestamp = $current_time;
	    $geo_to_insert[] = $primary_attr['lon'] . ',' . $primary_attr['lat'];
	
	  }
	}
  }
  }
  fclose($fp);
  echo "\ninserted $insertCount rows\n";
  echo "This script ran in " . (time()-$bgtime) . " seconds\n";
  echo "\nDone.\n";
  
  function save_to_csv($plowID, $datestamp, $geo_to_insert, $fp) {
  
	$kml = "<LineString><tessellate>1</tessellate>
  			<coordinates>
 ";
	        
	    foreach ($geo_to_insert as $point)
	    	$kml = $kml . "			" . $point . ",0
";
	    
	    $kml = $kml . "</coordinates></LineString>";
		
		$insertArray = array(
    	"Plow ID" => $plowID,
    	"Datestamp" => $datestamp,
    	"geometry" => $kml
    	);
		
		fputcsv($fp, $insertArray);
		
		//print_r($insertArray);
  }
?>