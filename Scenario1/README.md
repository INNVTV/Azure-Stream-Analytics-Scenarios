# Scenario 1
A series of low frequency readers set up at entrances and exits (gates) to rooms in a medical treatment center. Since the Covid-19 outbreak the facility wants to track times it is taking for staff to suit up as they enter the facility as well as overall exposure times (time spent within the facility).

## Goals
Track worker entry times and duration of time spent between gates.

## Notes

**Gate entry**

The gate devices will fire multiple times per worker as they pass through the readers. Data analysis should focus on getting a single point in time from the aggregate and use this to maerk the time that the worker passed through each gate.

**Duration between gates**

Time spent in the area between each gate must also be tracked. Once we have a timestamp for entry between gate-1, gate-2 f, etc.  we must find the timespan between each of those markers.


## Event Data

**C# Model**

    var telemetryDataPoint = new
    {
        workerId = worker1Id,
        gateId = gateId,
        timeEntered = DateTime.UtcNow
    };

**Sample Data (Use event producer project to generate entire set)**	


| workerId | gateId | timeEntered | EventProcessedUtcTime | PartitionId | EventEnqueuedUtcTime |
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
| "43081665-c402-476a-b5b5-fe62da967a53" | "3" | "2020-04-20T16:47:41.9777414Z" |"2020-04-20T16:51:46.3526125Z" | 7 | "2020-04-20T16:47:40.1360000Z" |
| "1d1d040c-7a78-4ca8-ba6d-64f898513005" | "1" | "2020-04-20T16:46:43.5349608Z" |"2020-04-20T16:51:46.3526125Z" | 7 | "2020-04-20T16:47:40.1360000Z" |
| "1d1d040c-7a78-4ca8-ba6d-64f898513005" | "1" | "2020-04-20T16:42:26.2056277Z" |"2020-04-20T16:51:46.3526125Z" | 7 | "2020-04-20T16:47:40.1360000Z" |
| "f3eb9b98-cd24-429d-b11d-20c73cfdf6a8" | "2" | "2020-04-20T16:42:25.9528915Z" |"2020-04-20T16:51:46.3526125Z" | 7 | "2020-04-20T16:47:40.1360000Z" |
...

## Query 1

**Count of sensor readings for each worker within a window of time** 

    SELECT
        workerId,
        Count(*) AS Count
    INTO
        TableStore
    FROM
        GatesInput
    GROUP BY
        workerId,TumblingWindow(second, 5)
		
**OUTPUT**

| workerId | count |
| ------------- | ------------- | ------------- |
| "43081665-c402-476a-b5b5-fe62da967a53" | "88" | 
| "1d1d040c-7a78-4ca8-ba6d-64f898513005" | "45" | 
| "f3eb9b98-cd24-429d-b11d-20c73cfdf6a8" | "48" | 


## Query 2
	
**Get first sensor reading for each workerId at each gate within a time window**
This is likely the query that will be used to populate a storage account in order to have a function pull and calculate duration between gate readings for each worker.

    SELECT
        workerId,
        gateId,
        timeEntered
    INTO
        TableStore
    FROM
        GatesInput
    WHERE
        IsFirst(minute, 10) OVER (PARTITION By workerId, gateId) = 1
	
**OUTPUT**

| workerId | gateId | timeEntered |
| ------------- | ------------- | ------------- |
| "43081665-c402-476a-b5b5-fe62da967a53" | "3" | "2020-04-20T16:47:41.9777414Z" |
| "1d1d040c-7a78-4ca8-ba6d-64f898513005" | "1" | "2020-04-20T16:46:43.5349608Z" |
| "f3eb9b98-cd24-429d-b11d-20c73cfdf6a8" | "1" | "2020-04-20T16:42:26.2056277Z" |
| "43081665-c402-476a-b5b5-fe62da967a53" | "1" | "2020-04-20T16:42:25.9528915Z" |
| "43081665-c402-476a-b5b5-fe62da967a53" | "2" | "2020-04-20T16:46:36.6014477Z" |
| "f3eb9b98-cd24-429d-b11d-20c73cfdf6a8" | "2" | "2020-04-20T16:46:37.2017680Z" |
| "f3eb9b98-cd24-429d-b11d-20c73cfdf6a8" | "3" | "2020-04-20T16:47:39.5618751Z" |

## Query 3
	
**Get first sensor reading for a particular worker at each gate within a time window**


    SELECT
        workerId,
        gateId,
        timeEntered
    INTO
        TableStore
    FROM
        GatesInput
    WHERE
        IsFirst(minute, 10) OVER (PARTITION By workerId, gateId) = 1 and workerId = 'f3eb9b98-cd24-429d-b11d-20c73cfdf6a8'
	
**OUTPUT**

| workerId | gateId | timeEntered |
| ------------- | ------------- | ------------- |
| "f3eb9b98-cd24-429d-b11d-20c73cfdf6a8" | "1" | "2020-04-20T16:42:26.2056277Z" |
| "f3eb9b98-cd24-429d-b11d-20c73cfdf6a8" | "2" | "2020-04-20T16:46:37.2017680Z" |
| "f3eb9b98-cd24-429d-b11d-20c73cfdf6a8" | "3" | "2020-04-20T16:47:39.5618751Z" |




