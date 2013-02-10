<?php
  //see README for instructions
  error_reporting(E_ALL);
  ini_set("display_startup_errors",1);
  ini_set("display_errors",1);
  ini_set("memory_limit","200M"); //datasets we are dealing with can be quite large, need enough space in memory
  set_time_limit(0);
  date_default_timezone_set('America/Chicago');
  
  //inserting in to Fusion Tables with http://code.google.com/p/fusion-tables-client-php/
  require('source/clientlogin.php');
  require('source/sql.php');
  
  //my custom libraries
  require('source/connectioninfo.php');
  
  header('Content-type: text/plain');

  $fusionTableId = ConnectionInfo::$fusionTableId;
  
  echo "Plow Tracker to Fusion Tables import by Derek Eder\n\n";
  
  //Fetch data from Forest's OSM files
  chdir ("../osm/");
  $current_dir = getCwd();
  echo "Current directory is now $current_dir";
  
  while(true) {
  //keep track of script execution time
  $bgtime=time();
  $array = scandir(".", 1);
  print_r($array);
  
  //Fetch info from Fusion Tables and do inserts & data manipulation
    //get token
	$token = ClientLogin::getAuthToken(ConnectionInfo::$google_username, ConnectionInfo::$google_password);
	$ftclient = new FTClientLogin($token);
	
	//for clearing out table
	$ftclient->query("DELETE FROM $fusionTableId");
	
	//check how many are in Fusion Tables already
	//$ftResponse = $ftclient->query("SELECT Count() FROM $fusionTableId");
	//echo "$ftResponse \n";

  $current_time;
  $insertCount = 0;
  echo "\n----Inserting in to Fusion Tables----\n";
  foreach($array as $filename) 
  {	
    if (strpos($filename,'.osm') !== false) {
  	  echo "Processing $filename ...\n";
  	  @$xml = simplexml_load_file("$filename");
      $plowID = strstr($filename, '_', true);
      $latestInsert = get_latest_insert($plowID, $ftclient, $fusionTableId);
      $datestamp = "";
      $geo_to_insert = array();
  	  
  	  if ($xml != null) {
  		  foreach ($xml->xpath('/osm/node') as $node) {
    			$primary_attr = $node->attributes();   // returns an array
    			
    			foreach ($node->children() as $tag) {
    				$secondary_attr = $tag->attributes();
    				if ($secondary_attr['k'] == 'time') {
    					//if we come across a time, we know to insert the current set of collected values
    					$datestamp_as_date = new DateTime($datestamp);

    					if (!empty($geo_to_insert) && $datestamp_as_date > $latestInsert) {
    						//hack to add in the connecting point to the next line segment
    						$geo_to_insert[] = $primary_attr['lon'] . ',' . $primary_attr['lat'];
    						insert_to_ft($plowID, $datestamp, $geo_to_insert, $ftclient, $fusionTableId);
    						$insertCount++;
    						echo "inserted $insertCount so far\n";
                sleep(1);
    					}
    					$geo_to_insert = array();
    					$current_time = $secondary_attr['v'];
    				}
    			}
    			
    			$datestamp = $current_time;
    			$geo_to_insert[] = $primary_attr['lon'] . ',' . $primary_attr['lat'];
  		
  		  }
  	  }
  	}
  }

  echo "\ninserted $insertCount rows\n";
  echo "This script ran in " . (time()-$bgtime) . " seconds\n";
  echo "\nWaiting 30 seconds.\n";
  sleep(30);
  }

  function get_latest_insert($plowID, $ftclient, $fusionTableId) {
    //this part is very custom to this particular dataset. If you are using this, here's where the bulk of your work would be: data mapping!
    $ftResponse = $ftclient->query(SQLBuilder::select($fusionTableId, "'Datestamp'", "'Plow ID' = '$plowID'", "'Datestamp' DESC", "1"));
    //echo "Response: " . var_dump($ftResponse[1]) . "\n";
    if (count($ftResponse) > 1)
      $latestInsert = new DateTime($ftResponse[1][0]);   
    else
      $latestInsert = new DateTime("1/1/2001"); //if there are no rows, set it to an early date so we import everything
      
    echo "\nLatest FT insert for $plowID: " . $latestInsert->format('m/d/Y H:i:s') . "\n";
    return $latestInsert;
  }
  
  function insert_to_ft($plowID, $datestamp, $geo_to_insert, $ftclient, $fusionTableId) {
  		
  		$kml = "<LineString>
  		<tessellate>1</tessellate>
  			<coordinates>
 ";
	        
	    foreach ($geo_to_insert as $point)
	    	$kml = $kml . "			" . $point . ",0
";
	    
	    $kml = $kml . "		
	    	</coordinates>
		</LineString>";
	      
	    echo $plowID . ' - ' . $datestamp . PHP_EOL;
    	//echo $kml . PHP_EOL;
  		
  		$insertArray = array(
    	"Plow ID" => $plowID,
    	"Datestamp" => $datestamp,
    	"geometry" => $kml
    	
    	/*
    	KML line format
    	<LineString
	        <tessellate>1</tessellate>
	        <coordinates> -112.2550785337791,36.07954952145647,2357
	          -112.2549277039738,36.08117083492122,2357
	          -112.2552505069063,36.08260761307279,2357
	          -112.2564540158376,36.08395660588506,2357
	          -112.2580238976449,36.08511401044813,2357
	          -112.2595218489022,36.08584355239394,2357
	          -112.2608216347552,36.08612634548589,2357
	          -112.262073428656,36.08626019085147,2357
	          -112.2633204928495,36.08621519860091,2357
	          -112.2644963846444,36.08627897945274,2357
	          -112.2656969554589,36.08649599090644,2357 
	        </coordinates>
	      </LineString>
	    */
    	);
    
    	echo $ftclient->query(SQLBuilder::insert($fusionTableId, $insertArray));
  }
?>