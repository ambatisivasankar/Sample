-- If somehow the interaction table exists still, get rid of it
DROP TABLE IF EXISTS HAVEN_UW.INTERACTION;

-- Add interaction table back in to haven_uw schema
SELECT COPY_TABLE('SQUARK_STAGING.HAVEN_UW_INTERACTION_BACKUP', 'HAVEN_UW.INTERACTION');

-- Grants
GRANT ALL  PRIVILEGES  EXTEND  ON TABLE haven_uw.interaction  TO  haven_uw_admin;
GRANT SELECT  ON TABLE haven_uw.interaction  TO  haven_uw_view,haven_uw;
