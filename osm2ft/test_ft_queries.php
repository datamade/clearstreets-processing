<?php
  error_reporting(E_ALL);
  ini_set("display_startup_errors",1);
  ini_set("display_errors",1);
  
include('source/clientlogin.php');
include('source/sql.php');
include('source/connectioninfo.php');

//get token
$token = ClientLogin::getAuthToken(ConnectionInfo::$google_username, ConnectionInfo::$google_password);
$ftclient = new FTClientLogin($token);

echo 'login: ' . ConnectionInfo::$google_username . ' ' . ConnectionInfo::$google_password;
echo ' token: ' . $token;

//show all tables
//echo $ftclient->query(SQLBuilder::showTables());
//echo "<br />";
//describe a table
//echo $ftclient->query(SQLBuilder::describeTable(2699417));
//echo "<br />";
//select * from table
echo $ftclient->query(SQLBuilder::select(ConnectionInfo::$fusionTableId));
//echo "<br />";
//select * from table where test=1
//echo $ftclient->query(SQLBuilder::select(358077, null, "'test'=1"));
//echo "<br />";
//select test from table where test = 1

//$ftclient->query(SQLBuilder::select(564620, array('rowid'), "'ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)'=''"));

//foreach ($ftclient as $key => $value) {
//    echo "RowId: $value<br />\n";
//    }
//echo "<br />";
//select rowid from table
//echo $ftclient->query(SQLBuilder::select(358077, array('rowid')));
//echo "<br />";
//delete row 401
//echo $ftclient->query(SQLBuilder::delete(358077, '401'));
//echo "<br />";
//drop table
//echo $ftclient->query(SQLBuilder::dropTable(358731));
//echo "<br />";
//update table test=1 where rowid=1
//echo $ftclient->query("UPDATE 564620 SET 'ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)' = 2 WHERE ROWID = ''");
//echo "<br />";
//insert into table (test, test2, 'another test') values (12, 3.3333, 'bob')
echo $ftclient->query(SQLBuilder::insert(ConnectionInfo::$fusionTableId, array('Plow ID'=>1, 'Datestamp' => "1/19/2012", 'geometry' => 'xyz')));

?>