-- @description query01 produces an OutOfMemoryError in java and stops PXF
-- start_matchsubs
--
-- # create a match/subs
--
-- m/Check the PXF logs located in the.*/
-- s/Check the PXF logs located in the.*/Check the PXF logs located in the 'log' directory on host 'mdw' or 'set client_min_messages=LOG' for additional details./
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- m/Failed (to )?connect to/
-- s/Failed (to )?connect to.*/Failed to connect to server, must be down/
--
-- end_matchsubs

SELECT * from test_out_of_memory;

-- wait until the JVM has been killed
SELECT pg_sleep(3);

-- We expect to see a Failed to connect error
SELECT * from test_out_of_memory;
