CREATE TABLE IF NOT EXISTS seat (
                                    id INT PRIMARY KEY,
                                    reserved BOOLEAN
);

INSERT INTO seat(id, reserved)
SELECT gs.id, false
FROM generate_series(1, 1000) AS gs(id)
WHERE NOT EXISTS (SELECT 1 FROM seat WHERE seat.id = gs.id);
