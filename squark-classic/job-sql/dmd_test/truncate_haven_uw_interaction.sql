-- Get rid of the current HAVEN_INTERACTION_INCR table
DROP TABLE IF EXISTS dmd_test.haven_uw_interaction;

-- And re-create it
CREATE TABLE IF NOT EXISTS dmd_test.haven_uw_interaction LIKE haven_uw.interaction INCLUDING PROJECTIONS INCLUDE PRIVILEGES;

-- Grants
GRANT ALL  PRIVILEGES  EXTEND  ON TABLE dmd_test.haven_uw_interaction  TO  dmd_test_admin;
