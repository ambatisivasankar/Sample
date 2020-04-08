-- Get rid of the current HAVEN_INTERACTION_INCR table
DROP TABLE IF EXISTS dmd_test.haven_interaction;

-- And re-create it
CREATE TABLE IF NOT EXISTS dmd_test.haven_interaction LIKE haven.interaction INCLUDING PROJECTIONS INCLUDE PRIVILEGES;

-- Grants
GRANT ALL  PRIVILEGES  EXTEND  ON TABLE dmd_test.haven_interaction  TO  dmd_test_admin;
