-- Question 4. Tumbling window - pickup location
CREATE TABLE processed_events_aggregated (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);

SELECT PULocationID, num_trips
FROM processed_events_aggregated
ORDER BY num_trips DESC
LIMIT 3;

-- Question 5. Session window - longest streak
CREATE TABLE processed_events_session (
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    PULocationID  INTEGER,
    num_trips     BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);

SELECT PULocationID, window_start, window_end, num_trips
FROM processed_events_session
ORDER BY num_trips DESC
LIMIT 1;

-- Question 6. Tumbling window - largest tip
CREATE TABLE tip_per_hour (
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    total_tip     DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);

SELECT window_start, window_end, total_tip
FROM tip_per_hour
ORDER BY total_tip DESC
LIMIT 1;