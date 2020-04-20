# Azure Stream Analytics
Various scenarios showcasing EventHub data, Azure Stream Analytics queries and output.

# Secnario 1
Multiple gates firing events as workers pass through.

## Goals
Track worker entry times and duration of time spent between gates.

## Event Data

**C# Model**

    var event = new
    {
        workerId = worker1Id,
        gateId = gateId,
        timeEntered = DateTime.UtcNow
    };

**Sample Data**	


| workerId | gateId | timeEntered | EventProcessedUtcTime | PartitionId | EventEnqueuedUtcTime
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
| "43081665-c402-476a-b5b5-fe62da967a53" | "3" | "2020-04-20T16:47:41.9777414Z" |"2020-04-20T16:51:46.3526125Z" | 7 | "2020-04-20T16:47:40.1360000Z" |
| "1d1d040c-7a78-4ca8-ba6d-64f898513005" | "1" | "2020-04-20T16:46:43.5349608Z" |"2020-04-20T16:51:46.3526125Z" | 7 | "2020-04-20T16:47:40.1360000Z" |
| "1d1d040c-7a78-4ca8-ba6d-64f898513005" | "1" | "2020-04-20T16:42:26.2056277Z" |"2020-04-20T16:51:46.3526125Z" | 7 | "2020-04-20T16:47:40.1360000Z" |
| "f3eb9b98-cd24-429d-b11d-20c73cfdf6a8" | "2" | "2020-04-20T16:42:25.9528915Z" |"2020-04-20T16:51:46.3526125Z" | 7 | "2020-04-20T16:47:40.1360000Z" |

## Queries

**GROUP BY WINDOW OF TIME** 
    SELECT
        workerId,
        Count(*) AS Count
    INTO
        TableStore
    FROM
        GatesInput
    GROUP BY
        workerId,TumblingWindow(second, 5)
	
	
**GET FIRST IN WINDOW for EACH workerId at each gate**
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
	
	
	
	
	